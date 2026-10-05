# Vayu — Data Profile (OpenAQ v3)

> Explored on 3 October 2026 using `curl` + `jq` against the OpenAQ v3 API (~15 requests).
> Every query used, and what it revealed, is listed in [Appendix A](#appendix-a--exploration-queries).
> Scope: **NCR only** — 25 km radius around central Gurugram (28.4595, 77.0266), **reference monitors only**.

This document records what the source data actually looks like, the problems found in it, and the pipeline design rule each finding led to.

---

## 1. How OpenAQ organises data

| Level | What it is | Example |
|---|---|---|
| **Location** | A monitoring station at a physical place | Sector-51, Gurugram - HSPCB (id `10825`) |
| **Sensor** | One instrument at a station, measuring exactly one pollutant | PM2.5 sensor `12236259` at Sector-51 |
| **Measurement** | A single reading from one sensor for one time period | 101.0 µg/m³ for 00:00–01:00 IST, 1 Sep 2026 |

The pipeline fetches data **per sensor**, so the number of sensors in scope determines the number of API requests.

---

## 2. Scope

| Item | Value |
|---|---|
| Area | 25 km radius around central Gurugram (API maximum radius) |
| Station type | Reference-grade government monitors only (`location.isMonitor == true`) |
| Pollutants | PM10 (id 1), PM2.5 (id 2), NO2 in ppb (id 15) |
| History | From 2026-04-01 to present |
| Excluded | Low-cost sensors (AirGradient, Clarity) |

**Station counts (all types, before the monitors-only filter):**

| Measure | Count |
|---|---|
| Stations within 25 km | 45 |
| Stations reporting since 2026-04-01 | 33 |
| Candidate sensors at those stations (params 1, 2, 15) | 123 *(upper bound — includes dead old-feed sensors and low-cost devices)* |

Removing ~5 low-cost stations leaves **~28 active reference stations**, and removing dead old-feed sensors leaves an expected **~80 active sensors**. *(Exact monitors-only count: pending verification.)*

---

## 3. Parameters (pollutants)

| ID | Name | Units | Used? |
|---|---|---|---|
| 1 | pm10 | µg/m³ | ✅ |
| 2 | pm25 | µg/m³ | ✅ |
| 5 | no2 | µg/m³ | ❌ old NCR feed only (ended Oct 2022) |
| 7 | no2 | ppm | ❌ |
| 15 | no2 | ppb | ✅ current NCR feed |

---

## 4. Findings

### F1 — The same pollutant exists in multiple units
NO2 appears three times (IDs 5, 7, 15) with units µg/m³, ppm and ppb.

**Impact:** Mixing units would make averages meaningless, and unit conversion for gases depends on temperature and pressure.
**Rule:** Request parameters **by ID, never by name**. Carry units through every layer. Any conversion happens explicitly in dbt and is documented.

### F2 — Stations have duplicate sensors per pollutant (old feed vs new feed)
Most government stations list two sensors for the same pollutant. Their active periods at Sector-51:

| Sensor | Pollutant | Units | First reading | Last reading |
|---|---|---|---|---|
| 36181 | PM2.5 | µg/m³ | 2020-03-11 | **2022-10-16** |
| 12236259 | PM2.5 | µg/m³ | 2025-02-18 | 2026-10-02 (current) |
| 36178 | NO2 | µg/m³ | 2020-03-11 | **2022-10-16** |
| 12236256 | NO2 | ppb | 2025-02-18 | 2026-10-02 (current) |

The pairs never overlap: the old feed ended before the new one began.

**Impact:** Fetching both wastes requests on dead sensors. If two sensors ever did overlap, readings would be double-counted at station level.
**Rule:** Only fetch sensors whose active period (`datetimeFirst` → `datetimeLast`) **overlaps the date range being loaded**.

### F3 — Units changed when the feed changed
The old NO2 sensor reported µg/m³ (ID 5); its replacement reports ppb (ID 15).

**Rule:** Same as F1 — units are part of the parameter's identity. Indian standards (NAAQS) express NO2 in µg/m³; if comparison is needed, convert in dbt using ≈1.88 µg/m³ per ppb (25 °C, 1 atm), documented as an approximation.

### F4 — Some stations have stopped reporting
| Station | Last reading |
|---|---|
| Lodhi Road, Delhi - IITM | 2025-12-27 |
| New Moti Bagh, Delhi - MHUA | 2026-06-13 |

12 of 45 stations in the area have not reported since 1 April 2026, but their sensors are still listed by the API.

**Rule:** Covered by F2's activity-overlap rule — dead sensors are skipped automatically.

### F5 — Long historical gaps
Sector-51 has no data from either feed between Oct 2022 and Feb 2025.

**Impact:** None for the current scope (2026 onwards), but long-range history for this station is incomplete.

### F6 — Government data arrives about one day late, in batches
On 3 Oct 2026 (~13:00 UTC), every government station showed the same last reading: `2026-10-02T08:30:00Z`.

**Impact:** A daily run that fetches "yesterday" exactly once will miss readings that arrive late.
**Rule:** Incremental runs re-fetch a **lookback window** of recent days (3 days initially). Writes must be idempotent (overwrite, not append) so re-fetching never creates duplicates.

### F7 — Hours can be missing
One day of hourly data for sensor `12236259` returned **23 of 24** hours.
A full month for sensor 12236259 (September 2026) returned 683 of 720 hourly readings: about 5% missing
**Rule:** Never assume a complete hourly series. Missing hours stay missing; they are not filled with zeros or interpolated.

### F8 — Each hourly value is an aggregate with a quality score
The `/hours` endpoint returns an average of finer readings, plus metadata:

- `coverage.expectedCount: 4` and `observedCount: 4` mean the station measures every 15 minutes, and all 4 readings arrived.
- `coverage.percentComplete: 100.0` gives the share of expected readings received.
- `summary` gives min / median / max / avg / sd within the hour.
- `flagInfo.hasFlags` indicates whether OpenAQ has flagged the reading.

**Rule (Spark validation):** Quarantine to `rejected/` with a reason code when:
- `percentComplete` is below 75% → `low_coverage`
- `flagInfo.hasFlags` is true → `flagged_by_source`
- value is negative or outside the configured range → `out_of_range`

### F9 — Time handling: periods, UTC and IST
Each reading describes a period, with UTC and local times:

```
period.datetimeFrom.utc   = 2026-08-31T18:30:00Z
period.datetimeFrom.local = 2026-09-01T00:00:00+05:30
```

- India is UTC+5:30, so hourly periods start on the **half-hour in UTC** and on the **whole hour in IST**.
- Requesting `datetime_from=2026-09-01` returned a first period starting at midnight **IST**, not midnight UTC, which suggests the API interprets these dates in station-local time (observed from one example).

**Rules:**
- Store all timestamps in **UTC**; derive local date/hour (`Asia/Kolkata`) for `dim_date`, so a "daily average" means the Indian calendar day.
- Compute our own UTC period-start for every reading, and **deduplicate on (sensor_id, period_start_utc)**, so date-boundary quirks can never create duplicates.

### F10 — Readings don't carry coordinates
`coordinates` is `null` on hourly readings.

**Rule:** Location attributes (name, coordinates, locality, provider) come from location metadata, in `dim_location`, joined to facts via sensor → location.

### F11 — Reference monitors and low-cost sensors are mixed together
- **Reference monitors** (CPCB / DPCC / HSPCB / IITM / IMD): government-grade, consistent 15-minute measurement.
- **Low-cost sensors** (AirGradient, Clarity): less accurate, especially in high humidity; different reporting patterns; includes at least one calibration unit ("NASA GSFC Rutgers Calib. N8").

**Rule:** Exclude known low-cost providers (`AirGradient`, `Clarity`), set in the config. *(Originally `isMonitor == true`; replaced after F16.)*

### F12 — Metadata is incomplete
Several DPCC stations report `"provider": "N/A"`.

**Rules:**
- Do not use the provider name to classify stations — use `isMonitor` (see F11).
- Dimensions map missing descriptive values to an explicit `Unknown` member rather than dropping rows.

### F13 — `meta.found` is not always a number
With `limit=1`, India's location count came back as the string `">1"`, meaning only "more than your limit". With `limit=1000` it returned `764`.

**Rule:** Never trust `meta.found` as a count. Paginate until a page returns fewer results than the limit.

### F14 — Rate-limit headers only describe the per-minute window
Observed response headers:

```
x-ratelimit-limit: 60
x-ratelimit-used: 2
x-ratelimit-remaining: 58
x-ratelimit-reset: 40      (seconds until the window resets)
```

The hourly limit (2,000) is not reported anywhere in the response.

**Rules:**
- Our own pacing (1 request every 2 s) enforces the hourly limit.
- Headers act as a safety net for the per-minute limit: pause for `reset` seconds when `remaining` drops below 3.
- On HTTP 429, wait `reset` seconds plus a buffer, then retry with exponential backoff.

---
### F16 — `isMonitor` is unreliable; 25% of government stations are unflagged
Checking what the `isMonitor` filter excluded revealed 7 government stations marked `isMonitor: false`:
New Moti Bagh (MHUA), IIT Delhi (IITM), and five DPCC stations (Talkatora Garden, JNU, NSUT Jaffarpur, IGNOU Maidan Garhi, Cantonment Area).
They share owner `OpenAQ admin`, instruments `N/A`, and (mostly) provider `N/A`, which suggests manually entered, incomplete metadata.

| Field | Separates the 7 from low-cost devices? |
|---|---|
| `isMonitor` | No — both `false` |
| `instruments` | No — the 7 show `N/A` |
| `owner` | No — `OpenAQ admin` appears on both sides |
| `provider` | **Yes** — low-cost devices are always `AirGradient` or `Clarity` |

**Impact:** The original filter silently dropped 7 of 28 active government stations (25% of the network) with no error.
**Rules:**
- Classify by excluding known low-cost providers (a denylist), not by `isMonitor` (an allowlist). A denylist fails *visibly* (a new provider appears in the output); the allowlist failed *invisibly*.
- Discovery reports every dropped station with a reason on every run.

### F17 — Completeness must be measured within each sensor's active window
Measured against full calendar months, the least complete sensor-month was New Moti Bagh in June (40.7%).
But the station stopped on 13 June: it delivered 293 of ~297 possible hours, nearly complete for its lifetime.

**Rule:** Expected hours = hours from max(month start, first reading) to min(month end, last reading),
not the full month.

### F18 — Gaps occur at instrument level and station level
From the first full load (Apr–Sep 2026), the least complete active sensor-months:
- **Cantonment Area (DPCC)** — PM10 only, June: 43%. Other sensors unaffected → instrument outage.
- **Teri Gram (HSPCB)** — PM10, PM2.5 and NO2 all at 57–61% in May and June → station-wide outage.

No completed month had zero readings for an active sensor.

**Rules:**
- dbt computes **data capture** (hours present / expected hours, F17) per sensor per day and month.
- Averages with capture below **75%** are flagged *insufficient data* rather than shown as normal values
  (a common air-quality convention, e.g. US EPA).
- This complements F8: F8 checks completeness *within* an hour (Spark); F18 checks completeness *across*
  hours (dbt).

## 5. Sample raw record (`/v3/sensors/12236259/hours`)

```json
{
  "value": 101.0,
  "flagInfo": { "hasFlags": false },
  "parameter": { "id": 2, "name": "pm25", "units": "µg/m³", "displayName": null },
  "period": {
    "label": "1hour",
    "interval": "01:00:00",
    "datetimeFrom": { "utc": "2026-08-31T18:30:00Z", "local": "2026-09-01T00:00:00+05:30" },
    "datetimeTo":   { "utc": "2026-08-31T19:30:00Z", "local": "2026-09-01T01:00:00+05:30" }
  },
  "coordinates": null,
  "summary": { "min": 83.3, "median": 103.35, "max": 115.8, "avg": 101.45, "sd": 16.86 },
  "coverage": {
    "expectedCount": 4,
    "observedCount": 4,
    "percentComplete": 100.0,
    "percentCoverage": 100.0
  }
}
```
*(Some fields trimmed for readability.)*

**Fact table grain:** one row per **sensor** per **hour**. Each sensor measures exactly one parameter, so "per sensor" already implies "per parameter".

---

## 6. Request budget

**API limits:** 60 requests/minute **and** 2,000 requests/hour, shared by everything using the API key.

**Cost model:**
- One request returns up to 1,000 results.
- One sensor produces ≤ 744 hourly readings per month, which fits in one page.
- So **1 sensor × 1 month = 1 request**, and **requests ≈ sensors × months + metadata**.

**Safe pace:** 2,000 / 60 min ≈ 33 requests/min ≈ one every 1.8 s, so we use **one request every 2 seconds**, which satisfies both limits indefinitely.

| Run | Sensors | Requests | Time at 1 req / 2 s | Binding limit |
|---|---|---|---|---|
| First load (6 months) | ~80 | ~480 data + ~29 metadata ≈ **510** | **~17 min** | per-minute only |
| Daily incremental | ~80 | ~80 | ~3 min | none |

Metadata = 1 request for the station list + 1 per active station (to get each sensor's first/last reading dates).

**Expected volume:** ~80 sensors × 24 hours × 183 days ≈ **350,000 rows** for the first load.

**Why `/hours` and not `/measurements`:** raw 15-minute data is ~2,976 readings per sensor-month (3 pages). Hourly aggregates need 1 page, cutting requests ~3×, and match the fact table's grain. Tradeoff: no sub-hourly detail.

---

## 7. Design rules derived from this profile

| Finding | Rule | Implemented in |
|---|---|---|
| F1, F3 | Select parameters by ID (1, 2, 15); units travel with every reading | Config, extraction, dbt |
| F2, F4 | Only fetch sensors active during the load window | Extraction (sensor discovery) |
| F6 | 3-day lookback on incremental runs; overwrite partitions | Extraction, watermarks |
| F7 | No gap-filling | Spark, dbt |
| F8 | Quarantine low-coverage, flagged and out-of-range values with reason codes | Spark → `rejected/` |
| F9 | Store UTC, derive IST for dates; dedupe on (sensor, period start) | Spark, `dim_date` |
| F10, F12 | Location attributes from metadata; explicit `Unknown` members | dbt dimensions |
| F11, F12 | Reference monitors only, via `isMonitor` | Config, extraction |
| F13 | Paginate by page size, not `meta.found` | Extraction |
| F14 | 1 request / 2 s; header-based pause; backoff on 429; Airflow pool with 1 slot | Extraction, Airflow |
| F15 | A realistic missing ID returns 404; only an out-of-range ID (999999999999) returned 500, likely a server-side integer overflow.

---

## 8. Decisions made

- [x] **Scope:** NCR only (25 km around Gurugram)
- [x] **Stations:** reference monitors only, classified by `isMonitor` (provider field is unreliable, F12)
- [x] **Parameters:** 1 (PM10), 2 (PM2.5), 15 (NO2 ppb)
- [x] **History start:** 2026-04-01
- [x] **Coverage threshold:** 75% (initial)
- [x] **Lookback window:** 3 days (initial)
- [x] **Rate-limit headers:** `x-ratelimit-limit`, `-used`, `-remaining`, `-reset` (per-minute window only)

## 9. Open questions

- [ ] Exact count of active reference-monitor sensors in NCR
- [ ] Review `value_ranges` upper bounds after the first load
- [ ] Confirm the 75% coverage threshold against the real distribution of `percentComplete`
- [ ] Report NO2 in ppb, or add a converted µg/m³ column in dbt for NAAQS comparison?

---

## Appendix A — Exploration queries

All queries run from the project folder in the Ubuntu terminal. The API key is never typed into a command; it is loaded from `.env`:

```bash
sudo apt install -y jq    # JSON viewer, used to select and format fields
source .env               # loads OPENAQ_API_KEY into the terminal session
```

Every request sends the key in the `X-API-Key` header. `curl -s` hides the progress bar; `jq` picks out the fields we care about.

---

### Q1 — Verify the API key works

**Question:** Is the key valid, and what does a response look like?

```bash
curl -i -H "X-API-Key: $OPENAQ_API_KEY" "https://api.openaq.org/v3/locations?limit=1"
```

`-i` shows response headers as well as the body.

**Result:** HTTP `200` with one location in JSON.

---

### Q2 — Find pollutant IDs and units

**Question:** What IDs does OpenAQ use for PM2.5, PM10 and NO2?

```bash
curl -s -H "X-API-Key: $OPENAQ_API_KEY" "https://api.openaq.org/v3/parameters?limit=100" \
  | jq '.results[] | select(.name=="pm25" or .name=="pm10" or .name=="no2") | {id, name, units}'
```

`select(...)` keeps only the three pollutants; `{id, name, units}` shows just those fields.

**Result:** PM10 = 1, PM2.5 = 2, and NO2 appears **three times**: 5 (µg/m³), 7 (ppm), 15 (ppb).
**Led to:** F1 — select parameters by ID, never by name.

---

### Q3 — List stations near Gurugram with their sensors

**Question:** Which stations exist within 25 km, what do they measure, and are they still reporting?

```bash
curl -s -H "X-API-Key: $OPENAQ_API_KEY" \
  "https://api.openaq.org/v3/locations?coordinates=28.4595,77.0266&radius=25000&limit=100" \
  | jq '{found: .meta.found, stations: [.results[] | {id, name, provider: .provider.name, last_reading: .datetimeLast.utc, sensors: [.sensors[] | {id, pollutant: .parameter.name}]}]}'
```

`coordinates` is `latitude,longitude`; `radius` is in metres (API maximum 25,000).

**Result:**
- Most government stations list **two sensors per pollutant** (e.g. PM2.5 `36181` and `12236259` at Sector-51).
- Some stations stopped reporting (Lodhi Road: Dec 2025; New Moti Bagh: Jun 2026).
- All government stations shared the same last reading, `2026-10-02T08:30:00Z`, about a day behind.
- Low-cost sensors (AirGradient, Clarity) were near real-time, including a calibration unit.
- Several stations had `provider: "N/A"`.

**Led to:** F2, F4, F6, F11, F12.

---

### Q4 — Compare the duplicate sensors

**Question:** Are the two sensors per pollutant both live, or is one an old feed?

```bash
for id in 36181 12236259 36178 12236256; do
  curl -s -H "X-API-Key: $OPENAQ_API_KEY" "https://api.openaq.org/v3/sensors/$id" \
    | jq '.results[0] | {id, name, parameter_id: .parameter.id, units: .parameter.units, first: .datetimeFirst.utc, last: .datetimeLast.utc}'
done
```

A bash `for` loop runs the same request once per sensor ID.

**Result:** Old sensors (`36181`, `36178`) ran 2020-03 → 2022-10; new sensors (`12236259`, `12236256`) run 2025-02 → present. No overlap. The NO2 units changed from µg/m³ (ID 5) to ppb (ID 15).
**Led to:** F2, F3, F5 — fetch only sensors active during the load window; use parameter 15 for NO2.

---

### Q5 — Inspect one day of hourly readings

**Question:** What does a single reading look like, and are hours ever missing?

```bash
curl -s -H "X-API-Key: $OPENAQ_API_KEY" \
  "https://api.openaq.org/v3/sensors/12236259/hours?datetime_from=2026-09-01&datetime_to=2026-09-02&limit=24" \
  | jq '{meta: .meta, readings_returned: (.results | length), first_reading: .results[0]}'
```

`(.results | length)` counts the readings returned.

**Result:**
- **23 of 24** hours returned — gaps happen.
- Each hourly value is an average of four 15-minute readings, with a `coverage` block (`percentComplete`) and `flagInfo`.
- Periods are given in UTC and local time; the first period started at midnight **IST** (18:30 UTC the previous day).
- `coordinates` is `null` on readings.

**Led to:** F7, F8, F9, F10, and the fact-table grain (one row per sensor per hour). The full record is in Section 5.

> **Note:** a first attempt left the placeholder `SENSOR_ID` in the URL. The API returned a plain-text error, and `jq` failed with `Cannot index string with string "meta"`. Lesson: when `jq` can't index a string, the API sent back an error message, not JSON data.

---

### Q6 — Count stations in India

**Question:** How large is the national dataset?

```bash
# Attempt 1
curl -s -H "X-API-Key: $OPENAQ_API_KEY" \
  "https://api.openaq.org/v3/locations?iso=IN&limit=1" | jq '.meta.found'

# Attempt 2
curl -s -H "X-API-Key: $OPENAQ_API_KEY" \
  "https://api.openaq.org/v3/locations?iso=IN&limit=1000" \
  | jq '{found: .meta.found, on_this_page: (.results | length)}'
```

**Result:** Attempt 1 returned the string `">1"` (only "more than your limit"). Attempt 2 returned **764**.
**Led to:** F13 — paginate by page size, never trust `meta.found` as a count. (National scope was later ruled out; NCR only.)

---

### Q7 — Count active stations and candidate sensors in NCR

**Question:** How many sensors would the pipeline need to fetch?

```bash
curl -s -H "X-API-Key: $OPENAQ_API_KEY" \
  "https://api.openaq.org/v3/locations?coordinates=28.4595,77.0266&radius=25000&limit=1000" \
  | jq '{stations: (.results | length),
         active_stations: ([.results[] | select(.datetimeLast.utc >= "2026-04-01")] | length),
         candidate_sensors: ([.results[] | select(.datetimeLast.utc >= "2026-04-01") | .sensors[] | select(.parameter.id == 1 or .parameter.id == 2 or .parameter.id == 15)] | length)}'
```

ISO timestamps compare correctly as text, so `>= "2026-04-01"` keeps stations active since April.

**Result:** 45 stations, 33 active, 123 candidate sensors (upper bound: includes dead old-feed sensors and low-cost devices).
**Led to:** the request budget in Section 6.

---

### Q8 — Inspect the rate-limit headers

**Question:** What does the API tell us about our remaining allowance?

```bash
curl -s -D - -o /dev/null -H "X-API-Key: $OPENAQ_API_KEY" \
  "https://api.openaq.org/v3/locations?limit=1" | grep -i ratelimit
```

`-D -` prints the headers; `-o /dev/null` discards the body; `grep -i ratelimit` keeps only the rate-limit lines.

**Result:**
```
x-ratelimit-limit: 60
x-ratelimit-used: 2
x-ratelimit-remaining: 58
x-ratelimit-reset: 40
```
**Led to:** F14 — headers only describe the per-minute window, so the pipeline enforces the hourly limit with its own pacing.

---

### Q9 — Count active reference-monitor sensors in NCR

**Question:** After restricting to reference monitors, how many sensors are in scope?

```bash
curl -s -H "X-API-Key: $OPENAQ_API_KEY" \
  "https://api.openaq.org/v3/locations?coordinates=28.4595,77.0266&radius=25000&limit=1000" \
  | jq '{monitor_stations: ([.results[] | select(.isMonitor == true)] | length),
         active_monitor_stations: ([.results[] | select(.isMonitor == true and .datetimeLast.utc >= "2026-04-01")] | length),
         candidate_sensors: ([.results[] | select(.isMonitor == true and .datetimeLast.utc >= "2026-04-01") | .sensors[] | select(.parameter.id == 1 or .parameter.id == 2 or .parameter.id == 15)] | length)}'
```

**Result:** *Pending.*
**Will confirm:** the final sensor count and request budget.

---

### API cost of this exploration

About 15 requests in total — under 1% of one hour's allowance (2,000).

---

*Data source: [OpenAQ](https://openaq.org). Original data providers include CPCB, DPCC, HSPCB, IITM and IMD.*