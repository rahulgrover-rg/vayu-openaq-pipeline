# Design decisions

Every significant choice in Vayu, why it was made, what else was considered, and what it costs.
Findings referenced as **F1–F30** are in [`docs/data-profile.md`](docs/data-profile.md).

Decisions that were **revised** during the project are marked *(revised)*, with what changed and why.
A project's mistakes are part of its design history.

---

## Contents

1. [Infrastructure](#1-infrastructure)
2. [Scope and source data](#2-scope-and-source-data)
3. [Ingestion](#3-ingestion)
4. [Processing (Spark)](#4-processing-spark)
5. [Warehouse (dbt and DuckDB)](#5-warehouse-dbt-and-duckdb)
6. [Data quality and monitoring](#6-data-quality-and-monitoring)
7. [Serving and BI](#7-serving-and-bi)
8. [Operations](#8-operations)
9. [Principles learned along the way](#9-principles-learned-along-the-way)

---

## 1. Infrastructure

### D1. Fully local, zero cost, one `docker compose up`
Every component runs in Docker on one laptop: no cloud accounts, no paid services.
- **Why:** reproducible by anyone, free, and each tool is interface-compatible with a managed
  equivalent (S3, EMR/Databricks, Snowflake, MWAA), so moving to cloud is configuration rather than
  rewriting.
- **Tradeoff:** single-machine limits everywhere (see README, *Limitations and scaling*).

### D2. WSL2, with the project inside the Linux filesystem
The project lives at `~/Projects/vayu` in Ubuntu, not on a Windows drive.
- **Why:** Docker reads Linux-filesystem files far faster, and Windows/Linux permission differences
  cause write errors on mounted folders (Airflow logs in particular).
- **Tradeoff:** after a reboot, containers can start before WSL's files are mounted; fixed with
  `docker compose up -d --force-recreate` (documented in the README).

### D3. Airflow 3.3 with the LocalExecutor; no Celery, Redis or triggerer
- **Why:** four Airflow containers instead of seven. Celery and Redis only matter when tasks run on
  several machines; the triggerer only serves deferrable operators, which this project doesn't use.
- **Alternative:** CeleryExecutor or KubernetesExecutor, the scale-out path.

### D4. One Postgres server, three separate databases
`airflow` (Airflow's metadata), `pipeline` (our extraction ledger), `metabase` (Metabase's settings).
- **Why:** isolation without extra containers. An Airflow upgrade can never touch the ledger;
  Metabase's dashboards survive container rebuilds.
- **Tradeoff:** one server is a shared point of failure.

### D5. Object storage: the `pgsty/minio` fork *(revised)*
- **Original decision:** pin the last official MinIO image.
- **What happened:** MinIO removed its images from Docker Hub in September 2026, and its Quay
  registry refused anonymous pulls (401).
- **Revised:** `pgsty/minio`, a maintained community fork (also used by ClickHouse's tests): the
  same S3 API, configuration and tooling.
- **Why it was cheap to change:** the code only talks to the S3 API, so the storage engine is a
  configuration detail.
- **Tradeoff:** a third-party fork depends on one maintainer. Exit plan: SeaweedFS, Garage or real S3.
- **Lesson:** pinning a version protects against *changes*, not against *removal*.

### D6. Secrets only in `.env`; choices in `config/pipeline.yaml`
- `.env` (never committed) holds passwords, the API key and Airflow's keys; `.env.example` lists the
  names with empty values.
- Airflow connections and variables are injected as environment variables
  (`AIRFLOW_CONN_*`, `AIRFLOW_VAR_*`), so no password appears in code, the UI or Git.
- `pipeline.yaml` (committed) holds scope, pacing and quality thresholds: configuration over
  hardcoding.

### D7. Local-only exposure: ports bound to 127.0.0.1, authentication disabled
- **Why:** nothing is reachable from the network, so local convenience (no Airflow login, MinIO root
  credentials in the pipeline) is acceptable.
- **Production would need:** real authentication and least-privilege service accounts instead of
  MinIO root credentials.

### D8. All mounted folders are created before the first start
`mkdir -p logs plugins .cache/ivy warehouse/serving` runs before `docker compose up`.
- **Why:** Docker creates missing mount folders as `root`, which Airflow's tasks (user 1000) cannot
  write to. Found when a deleted `serving/` folder was recreated as root after a reboot.

### D9. A permanent `smoke_test` DAG
- **Why:** triggering it answers "is the infrastructure healthy?" in 30 seconds, separating
  infrastructure problems from pipeline bugs.

### D10. Custom Airflow image; dbt in its own virtual environment
The image adds Java, PySpark, DuckDB and (in a separate venv) dbt to the official Airflow image.
`apache-airflow` is re-pinned during `pip install` so Airflow's version can't change by accident.
- **Why a separate venv for dbt:** dbt and Airflow depend on overlapping libraries at conflicting
  versions; isolation prevents one silently breaking the other.
- **Consequence:** Airflow runs dbt as a command (subprocess), not as an imported library (D50).
- **DuckDB is pinned to the same version (1.5.5) in both environments**, so they share one file
  format and one set of extensions.

---

## 2. Scope and source data

### D11. NCR only: 25 km around Gurugram
- **Why:** fast iteration (a full load in ~25 minutes), a manageable volume (~350,000 hourly
  readings), and a personally relevant story.
- **Alternatives considered:** all of India (764 locations; a multi-hour first load) and the original
  3–5 countries.
- **Tradeoff:** at this size, partitioning gains are modest (measured: within noise). Scaling is a
  configuration change (`scope` in `pipeline.yaml`).

### D12. Government reference monitors only, by excluding low-cost providers *(revised)*
- **Original decision:** keep stations where OpenAQ's `isMonitor` is true.
- **What happened (F16):** checking what the filter *removed* revealed 7 of 28 government stations
  with `isMonitor: false`: 25% of the network silently dropped, with no error and no failed test.
- **Revised:** exclude known low-cost providers (`AirGradient`, `Clarity`), listed in the config.
- **Why:** no positive "government" field is reliable, but low-cost devices always come from
  identifiable providers.
- **Tradeoff:** a new low-cost brand would be included until added to the list, but that failure is
  **visible** (a new provider appears in discovery's output), while the original failure was
  invisible.

### D13. Pollutants selected by ID: PM10 (1), PM2.5 (2), NO2 in ppb (15)
- **Why:** NO2 exists in three units (F1), and the NCR feed switched from µg/m³ to ppb when sensors
  were replaced (F3). Names are ambiguous; IDs aren't.

### D14. History from 1 April 2026
Covers the pre-monsoon, monsoon and post-monsoon seasons, and the start of crop-burning season.

### D15. The hourly endpoint (`/hours`), not raw measurements
- **Why:** a sensor-month of hourly values (≤ 744) fits in one API page; raw 15-minute data would
  need about three. Hourly also matches the fact table's grain.
- **Tradeoff:** no sub-hourly detail (the hour's minimum and maximum are kept, which F19 needed).

---

## 3. Ingestion

### D16. The API client is independent of Airflow
- **Why:** testable on its own (all failure paths were tested directly: 200, 404, 500, 401),
  reusable by discovery and extraction, and unaffected by a change of orchestrator.

### D17. Four failure classes; the client classifies, the task records
| Class | Examples | Handling |
|---|---|---|
| Transient | 429, 5xx, timeout | Retry with backoff; then `failed_transient`, retried next run |
| Permanent | 404 | `failed_permanent`, never retried |
| Poison | Invalid JSON, missing `results` | `poison`, never retried; other sensors continue |
| **Fatal** | 401/403, unexpected 4xx | **Stop the whole run** |
- **Why Fatal was added:** a wrong API key isn't one sensor's problem. Without it, every sensor-month
  would be recorded as a failure.
- **Known limitation (F15):** OpenAQ can return 500 for invalid input, so some "transient" errors
  never succeed. Bounded at 5 retries. Planned: escalate to permanent after repeated failures
  across runs.

### D18. Rate limiting: pacing first, headers as a safety net
- One request every 2 s keeps us under both limits indefinitely (2,000/hour ≈ 1 per 1.8 s).
- Rate-limit headers only describe the **per-minute** window (F14), so they can't enforce the
  hourly limit; they pause requests when the remaining count runs low.
- On 429: wait for the reset time given by the header; otherwise exponential backoff with jitter.
- Every request has a 30-second timeout.

### D19. An Airflow pool with one slot (`openaq_api`)
- **Why:** with dynamic task mapping, many tasks could call the API at once. Per-task pacing would
  then multiply the request rate. **Parallel tasks can't go faster than a shared rate limit.**
- **Tradeoff:** the pool lives in Airflow's database and must be recreated after a volume wipe
  (documented in the README).

### D20. Unit of work: one sensor-month
One request, one file (`raw/openaq/hours/year=…/month=…/sensor_id=….json`), one ledger row.
- **Why:** refetching a sensor-month overwrites the same file, so idempotency is built into the
  file layout.

### D21. One Airflow task per sensor, checkpoints per month
- **Why:** ~80 tasks instead of ~560. Airflow's per-task startup overhead is paid 80 times instead of
  560, roughly halving the runtime under a one-slot pool. The ledger still records every month, so
  no work is lost on failure.
- **Costs:** one task's status summarizes several months; the failing month is found in the ledger
  rather than the UI; forcing a refetch of a month means deleting ledger rows; timeouts must cover
  the worst-case backoff; longer histories would call for a middle granularity (per sensor-year).
- **Principle:** task granularity and checkpoint granularity are separate choices.

### D22. A ledger instead of a single watermark
`extract_ledger` has one row per sensor-month: status, row count, file path, HTTP status, error,
attempts, fetch time. Primary key `(sensor_id, period_month)`; writes are upserts.
- **Why:** a watermark only knows the latest point, so it can't represent a gap (month 5 failed,
  month 6 succeeded). A ledger records every unit, including failure types. A watermark can always
  be derived from it.

### D23. Daily cadence with open and closed months *(revised: lookback 3 → 7 days)*
- A month becomes final once fetched after **month end + lookback**. Until then it is refetched
  daily, capturing late-arriving data.
- **Alternatives considered:**
  - *Monthly* (fetch each month once after it closes): simplest, ~30× fewer requests, but the
    dashboard would be up to 5 weeks stale.
  - *Weekly* (Saturday–Friday, fetched on Tuesday, after the arrival window): no refetching, but
    4–11 days stale.
- **Why daily:** freshness matters for air quality, the request cost is under 1% of the budget, and
  handling late data is a core data-engineering skill.
- **Revision:** the lookback was set to 3 days from one measurement of normal latency (~1 day, F6).
  On 21 Oct 2026, an ~82-hour upstream outage showed that 3 days could lose month-end data, so it was
  widened to 7 days.

### D24. State-based planning, not schedule-based
Every run compares what *should* exist with what the ledger says *does*.
- **Why:** crashes, missed runs and weeks of downtime all heal with one normal run, with no special
  recovery code. Demonstrated by the resume test and a three-week catch-up in November.
- **Supporting settings:** `catchup=False` (one catch-up run after downtime, not one per missed day);
  `max_active_runs=1` (runs never compete for the API or the ledger); scheduled daily at 20:00 IST,
  when the machine is most likely on.

### D25. Discovery on every run; daily metadata snapshots in the raw zone
- **Why:** stations appear, die and briefly return (F26: Lodhi Road). A fixed sensor list would go
  stale silently.
- Each run saves the full station and sensor metadata to `raw/openaq/metadata/snapshot_date=…/`:
  the history behind the dimensions (D40).
- Every station dropped by discovery is reported with a reason (D12).
- **Known cost (F26):** planning from first/last readings can't see long gaps; a returning station
  costs a few empty requests once.

### D26. Write the file first, then record success; commit per month
- **Why:** a crash between the two writes leaves a file with no ledger row (refetched and overwritten
  next run), never a ledger row pointing at a missing file. At most one request is ever repeated.
- Empty months are recorded as successes with `row_count = 0`, so they're never re-requested once
  final.

### D27. Retries resume within a run *(revised)*
- **Original assumption:** an Airflow retry would skip the months already saved.
- **What was wrong:** a retry reruns the task with the same input, so it would refetch everything.
- **Revised:** each work item carries `planned_at`; extraction skips months saved successfully since
  then.

### D28. A transient failure stops that sensor, not the run
- **Why:** after 5 retries over about a minute, the API is probably struggling; the sensor's remaining
  months are left for the next run instead of each wasting another minute.
- **Future improvement:** a circuit breaker across tasks.

### D29. `summarize` reads the ledger, and fails the run when work is incomplete
- **Why read the ledger:** failed tasks produce no results; the ledger records every outcome.
- **Why fail:** Airflow decides a run's color from its last task. A quietly successful `summarize`
  would hide failed extractions behind a green run.
- `plan` returns the work list as its main value, because Airflow can only map over a task's main
  return value.

### D30. The raw zone stores responses untouched, in a provenance envelope
Endpoint, parameters, sensor, station, fetch time and row count wrap OpenAQ's unmodified readings.
- **Why:** Spark can be rerun on raw data at no API cost after any rule change, and "what did the
  source actually say?" can always be answered.

---

## 4. Processing (Spark)

### D31. Spark in local mode, inside the Airflow image
- **Why:** the fewest moving parts; the transformation code is unchanged on a cluster, Spark Connect
  or Databricks.
- **Alternatives:** separate Spark master/worker containers; a Spark Connect server; managed Spark
  (paid).
- **Tradeoff:** a heavier image; Spark shares memory with the scheduler; a single-machine assumption.
- **Honest note:** at this data size, DuckDB alone could do the cleaning. Spark is the part that
  scales without rewriting, and a skill the project set out to demonstrate.

### D32. Spark 4.1.2, with `hadoop-aws` derived from Spark's bundled Hadoop
- Spark 3.5 reached end of life in April 2026.
- The S3 connector must exactly match Spark's internal Hadoop version (a common source of
  `NoSuchMethodError`s), so the code reads the version from the installed PySpark instead of
  hardcoding it.
- The ~500 MB AWS SDK bundle is cached in a mounted folder, downloaded once.
- Spark's session time zone is fixed to UTC.

### D33. Explicit schemas, `try_` parsing, and per-row lineage
- **Explicit schemas:** no inference pass, and empty months can't change inferred types.
- **`try_to_timestamp` and friends:** Spark 4's ANSI mode would fail the whole job on one bad value;
  `try_` returns null, and validation rejects the row with a reason.
- **`source_file` on every row:** any reading can be traced to its raw file.

### D34. Deduplicate before validating; one primary reject reason
- Deduplicate on `(sensor_id, period_start_utc)`, keeping the most recently fetched copy, so the
  version judged is the latest one. Rows without a valid time skip deduplication, so they can't be
  miscounted as duplicates.
- Reject rules are checked in a fixed order; the first match is the reason, so reason counts always
  add up to the rejected total.
- 0 duplicates were found (F24), but the step is kept as protection against future overlaps.

### D35. Two-tier data quality: hard rejects and soft flags
| Tier | Meaning | Rules | Destination |
|---|---|---|---|
| Hard reject | **Wrong** | null; −10000; ±9999 inside an average; negative; beyond instrument range | `rejected/` |
| Soft flag | **Less reliable** | < 75% coverage; OpenAQ-flagged; at instrument ceiling | `curated/`, flagged |
- **Why:** quarantining 11% of valid readings for low coverage would discard usable data.
  "Quarantine" should mean wrong, not imperfect. Downstream models choose how to use flagged data.

### D36. Quality rules verified against the data before being coded
- **Why:** the first hypothesis for sentinel contamination (the hour's minimum is −10000) would have
  matched **nothing**: checking `summary.min` and `summary.max` revealed −9999 and +10000 (F19), and
  showed the two largest PM2.5 values were error codes, not pollution.

### D37. Quality thresholds come only from the config, with no defaults *(revised)*
- **What happened:** a fallback default (`min_percent_complete` defaulting to 0) silently disabled
  the low-coverage rule during development.
- **Revised:** required settings are read strictly (a missing key fails the run); only genuinely
  optional ones (instrument ceilings) may be absent. Unknown pollutant names in rules also fail fast.
- Transformation logic was reviewed through Spark's query plan (`explain()`), which caught a missing
  minus sign that would have rejected nearly every reading, with no error raised.

### D38. Source quality flags are an input, not the authority
OpenAQ's flags matched our null-value rejects exactly, but missed all 31 sentinel problems and 7 of 10
negatives: 38 invalid readings would have passed. They're kept as a soft flag.

### D39. UTC is canonical; Indian time derives dates and partitions
- OpenAQ's month windows follow IST (F9), so partitions and `dim_date` use local dates; timestamps
  stay in UTC.

### D40-a. Curated: snappy Parquet, partitioned by local year and month
- **One file per partition** (`repartition` by year and month) avoids the small-files problem. At
  larger scale, target files of roughly 128 MB to 1 GB.
- **Rows sorted** by sensor and time: tight min/max statistics (row-group skipping) and better
  compression.
- **Measured:** 42× smaller than the same data as JSON.
- **Full refresh with overwrite:** reruns are idempotent.
- **Known limitation:** overwrite on object storage isn't atomic; a crash mid-write leaves partial
  output until the next run. Delta Lake or Iceberg would provide atomic commits.
- **Rejected** is a single unpartitioned file (191 rows don't justify partitions).

### D40-b. Validate before publishing
Spark reconciles `readings = curated + rejected + duplicates` **before** writing, so a failed check
never overwrites good curated data, then reads the files back to verify the write.

### D41. A separate transform DAG, linked by an Airflow Asset
- **Why:** extraction is limited by the API, transformation by CPU; and after a rule change, the
  transform can be rerun alone, with no API calls.
- The asset is produced only when `summarize` succeeds, so the transform never runs on partial data.
- Inside tasks, credentials come from Airflow's connections, not raw environment variables.

### D42. `spark.sql.shuffle.partitions = 8` for local mode
- **Measured:** 200 → 8 cut the transform from ~41 s to ~37.5 s (~9%). About 12 s of each run is fixed
  Spark startup, so at this size startup and file reading dominate, not shuffles. At scale, size
  shuffle partitions from data volume (~100–200 MB each).

---

## 5. Warehouse (dbt and DuckDB)

### D43. DuckDB as the query engine
- Queries Parquet in MinIO in place, with no load step; starts in milliseconds versus Spark's ~12 s.
- S3 access through `CREATE SECRET` (the current API), not the older `SET s3_*` settings.
- The `httpfs` extension is installed at image build time, so tasks never download extensions.

### D44. dbt Core 1.11 (Python) with dbt-duckdb 1.10.1, not dbt v2
- dbt v2 reached GA in September 2026 with a built-in DuckDB adapter, but documentation and community
  knowledge are overwhelmingly for 1.x, and dbt-duckdb is mature for S3-hosted Parquet.
- Models and tests are portable to v2 later.
- Generic tests use the dbt 1.10+ `arguments:` syntax.

### D45. Sources in MinIO, staging as views, marts as tables
- Marts are physically stored, so consumers need only the warehouse file, not S3 credentials.
- `profiles.yml` contains only environment-variable references, so it's safe to commit.

### D46. Natural keys, not surrogate keys
OpenAQ's IDs and calendar dates.
- **Why:** one source, with stable IDs.
- **Revisit if:** a second source is added, or dimensions keep historical versions.

### D47. Reference data as seeds; standards converted, not readings
- Air-quality standards, the city mapping and manual overrides are small, reviewed CSVs in Git.
- NO2's standard is expressed in ppb (80 µg/m³ ≈ 42.6 ppb at 25 °C), so measured values stay
  untouched and the approximation lives in one documented place.

### D48. `dim_date` is generated, gap-free, from the data's own range
- From the first reading to the end of the following year; IMD seasons and an approximate
  crop-burning flag. No duplicate start-date setting.

### D49. Location and sensor dimensions: Type 1, across every station ever seen
- Each station once, with its most recently seen attributes; stations that left scope are kept, so
  historical facts never become orphans.
- **Reversible:** raw daily snapshots preserve full history, so Type 2 can be rebuilt later. (dbt's
  own snapshot feature would only record history from the day it started running.)
- City and agency are parsed from station names and mapped through a seed. An unmapped city fails the
  build (it would break city charts); an unknown agency only warns (it may be legitimate).

### D50. Station quietness is measured as of the snapshot, not the build date *(revised)*
- **Original:** `days_since_last_reading` used `current_date`.
- **What was wrong:** it mixed station lag with pipeline idle time (two days of laptop downtime would
  have marked every station "not reporting"), and the dimension changed daily without new data.
- **Revised:** `days_quiet_at_last_check`, against the snapshot date. Pipeline freshness is checked
  separately (D56).

### D51. Star schema, not snowflake; grain stated and tested
- **Grain:** one row per sensor per hour `(sensor_id, period_start_utc)`, proven by a uniqueness test.
- `location_id`, `parameter_id` and `date_day` are stored on the fact for one-step joins; a test
  checks they agree with `dim_sensor`.
- `relationships` tests act as foreign keys, which DuckDB tables in dbt don't enforce.

### D52. Incremental fact via `delete+insert` keyed on `source_file`
- Each run replaces **all rows from each refetched raw file**, so readings removed from a file (newly
  rejected or withdrawn) disappear too. Row-level merging couldn't remove them.
- **Why incremental at ~350,000 rows:** not for speed (a full build takes about a second), but as
  the pattern that scales, and a project requirement.
- **Known limitation:** a Spark rule change can reclassify old readings without changing
  `fetched_at`. Guarded by a fact-equals-curated count test; the fix is `--full-refresh`.
- `on_schema_change: fail`: schema changes require a deliberate full refresh.

### D53. `fact_sensor_daily`: an aggregate fact with three day states
- **Grain:** sensor × local day, **including days without data** (a spine joined to aggregates).
- `sufficient` (≥ 18 of 24 usable hours), `insufficient`, `no_data`. Daily values and exceedance flags
  are null unless sufficient: "unknown", not "no".
- **The active window comes from the facts** (first to last day with readings): silence inside counts
  as missing; silence at the edges (not started, stopped, or not yet arrived) doesn't. One definition
  handles F17, the F6 outage and F26.
- **Usable hours** exclude flatline hours (F29) and manually overridden periods (F30). They stay in
  `fact_measurement`, the source's raw record.
- *Layering exception:* this mart reads a quality model (`qa_flatlines`); documented in the model
  rather than adding an intermediate layer for one dependency.

### D54. The chain of counts is tested end to end
Ledger → raw → curated → fact → daily hours; dated rejects → incident rows. Each link is a test.

---

## 6. Data quality and monitoring

### D55. A separate quality layer (`models/quality/`)
Marts describe the air; quality models describe how far to trust the data, and feed dashboard caveats.
- `qa_network_days`: the newest day is always labelled `latest_incomplete`, because the daily batch
  cutoff leaves every station partial and would otherwise be misread as an outage every day.
- `qa_flatlines`: gaps-and-islands, 6-hour threshold (every top-20 run was ≥ 21 h, beyond rounding
  effects, F25).
- `qa_pm_consistency`: a **warning**, not an error, because the data can't tell which instrument is
  wrong (F22). An explicit test guarantees one sensor per station-pollutant-hour, so its pivot can't
  hide duplicates.
- Quality models cross-validate each other: capture, consistency and flatlines independently pointed
  at Cantonment Area's PM10 in June.

### D56. Freshness warns; it never fails
- dbt source freshness on `period_start_utc` (how current the **air** data is), warning at 48 h.
- A lag-near-lookback warning at 144 h, one day before the 7-day lookback, with the required human
  action written into the test.
- **Why warnings:** an upstream outage can't be fixed by stopping the pipeline, and the open-month
  design recovers the data once it arrives. The freshness check caught a live outage on its first run.
- **Known duplication:** `feed_lag_alert_hours` (dbt) depends on `lookback_days` (`pipeline.yaml`);
  documented in both.

### D57. Manual judgments live in a reviewed seed (`quality_overrides`)
- Each row has a sensor, a date range and a reason citing its finding; tested for valid sensors, valid
  ranges and a non-empty reason. Never hardcoded in SQL.
- First entry: Sector-51 NO2, 1 April – 19 June 2026 (F30), found by peer comparison, not by any
  automatic rule.

### D58. Test severity is a design choice
**Error** (stops the build) when the problem would make published numbers wrong: keys, grain,
relationships, counts, unmapped cities. **Warn** when it needs attention but not a stop: PM
consistency, unknown agencies, freshness, feed lag.

---

## 7. Serving and BI

### D59. dbt runs as one Airflow task, decided by its artifacts
- `dbt source freshness` and `dbt build` run as subprocesses; outcomes are read from
  `run_results.json` and `sources.json`, which are **deleted before each run** so a crashed run can
  never be reported with the previous run's results.
- **Alternative considered:** Astronomer Cosmos (one Airflow task per dbt model: per-model retries and
  visibility, at the cost of more tasks and setup).
- When a test fails, dbt skips dependent models, which **keep their last good version**: a failed
  run never degrades the warehouse. The failure path was tested deliberately with an invalid override.

### D60. Build → test → publish: Metabase reads a serving copy
- After every test passes, the warehouse's **tables** are copied to `warehouse/serving/vayu_serving.duckdb`
  (written to a temporary file, then renamed atomically).
- **Why:** DuckDB allows one writer per file; dbt and Metabase never share one. The dashboard only
  sees tested data. Views are excluded because they read MinIO and need S3 credentials.
- **Known limitation:** Metabase keeps reading the previous copy until it reconnects (restart after
  publishing). A production setup would serve BI from a database server readers reconnect to, or
  have the orchestrator notify the BI tool.

### D61. Metabase setup
- MotherDuck's pre-built image (Metabase + DuckDB driver), avoiding the glibc problem of
  Alpine-based images. The version-specific tag wasn't available, so the image is **pinned by
  digest** (tags can move).
- Its application database is Postgres, so dashboards survive rebuilds; memory capped at 1 GB.
- The serving folder is mounted **read-only**, with a read-only connection, so Metabase can never
  create, change or re-own files there.

### D62. A presentation table (`mart_station_day`) for BI
- The star schema stays the tested model; one wide table makes charts simple `select`s. Join logic
  lives in dbt, written once and tested, not repeated in each chart.
- A dbt **exposure** records the dashboard's dependencies in dbt's lineage.
- Chart SQL is versioned in `dashboard/queries.sql`, since free Metabase stores dashboards only in
  its own database.

### D63. Dashboard design choices
- District lines use **medians**, so one faulty station can't distort a district (F27).
- "Last 30 days **of data**", counted from the newest day rather than today, so outages don't empty
  the charts.
- Table colors follow **India's AQI breakpoints**, not a relative color range (which would show the
  least-polluted station green even when all are unhealthy).
- **City-level exceedance comparisons aren't headline numbers:** with 5 of 6 Gurugram stations
  showing an unexplained high PM2.5/PM10 ratio (F27), the dashboard compares stations and shows the
  ratio.
- **Caveats are on the dashboard itself**, plus a whole *Data quality* tab.
- *Known simplification:* the hour-of-day chart includes flatline and overridden hours (excluded only
  at daily grain).

---

## 8. Operations

### D64. The warehouse is disposable; the raw zone is the source of truth
On 12 Nov 2026 the `warehouse/` folder was accidentally deleted; one `vayu_transform` run rebuilt it
completely from curated data and the seeds, with every test passing. MinIO's volume is the one thing
that must never be deleted.

### D65. After a validation rule changes, run `dbt build --full-refresh` once
The incremental fact can't see reclassified old readings (D52); the drift test flags it if forgotten.

---

## 9. Principles learned along the way

These came from mistakes and surprises during the build. Each is linked to the decision it changed.

1. **Check what your filters remove, not just what they keep** (D12, F16). The worst bug in the
   project passed every test.
2. **Verify a hypothesis against the data before encoding it as a rule** (D36, F19).
3. **Between two imperfect rules, prefer the one whose failures are visible** (D12).
4. **A safety margin set from one measurement needs monitoring** (D23, D56, F6, F28).
5. **Quality settings have no defaults: a missing setting should stop the pipeline** (D37).
6. **Validate before you publish, at every layer** (D40-b, D59, D60).
7. **Measure rather than assume, and report small results honestly** (D42, partition pruning).
8. **When a fix doesn't work, look at the evidence again rather than repeating the fix**
   (a "folder in the way" was first blamed on Docker; file ownership and timestamps showed it was
   the code).
9. **Take documented numbers from the system's records, not from memory** (the resume test figures
   come from the plan task logs).
10. **Keep raw history, and modeling decisions stay reversible** (D49).