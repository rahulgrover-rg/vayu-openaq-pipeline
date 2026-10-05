import json  
import logging 
from datetime import datetime, timedelta, timezone 
from pathlib import Path 
import pendulum 
from airflow.sdk import Variable, dag, task 

log = logging.getLogger(__name__) 
POOL = "openaq_api" 
SQL_DIR = Path(__file__).parent / "sql" 

def _pg_conn(): 
    from airflow.providers.postgres.hooks.postgres import PostgresHook 
    return PostgresHook(postgres_conn_id="pipeline_db").get_conn() 

def _s3(): 
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
    return S3Hook(aws_conn_id="minio").get_conn() 

@dag(
    dag_id="vayu_extract" ,
    schedule="0 20 * * *" ,
    start_date=pendulum.datetime(2026,10,1,tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    tags=["vayu", "extract"], 
)
def vayu_extract(): 
    @task 
    def ensure_schema(): 
        from airflow.providers.postgres.hooks.postgres import PostgresHook 
        sql = (SQL_DIR / "create_extract_ledger.sql").read_text() 
        PostgresHook(postgres_conn_id="pipeline_db").run(sql) 

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def discover(): 
        from vayu.config import build_client, load_config 
        from vayu.discovery import discover_sensors 

        cfg = load_config() 
        client = build_client(cfg, Variable.get("OPENAQ_API_KEY"))
        result = discover_sensors(client,cfg) 

        now = datetime.now(timezone.utc) 
        key=f"openaq/metadata/snapshot_date={now:%Y-%m-%d}/stations.json" 
        snapshot = {"fetched_at": now.isoformat(), "scope": cfg["scope"], **result}
        _s3().put_object(
            Bucket=cfg["storage"]["raw_bucket"],
            Key=key, 
            Body=json.dumps(snapshot, ensure_ascii=False, default=str).encode("utf-8"),
            ContentType="application/json"
        )
        log.info("Saved metadata snapshot to s3://%s/%s", cfg["storage"]["raw_bucket"], key) 
        return result["sensors"]

    @task
    def plan(sensors) : 
        from vayu.config import load_config 
        from vayu.planning import load_ledger, plan_work 

        cfg = load_config() 
        conn = _pg_conn() 

        try: 
            ledger = load_ledger(conn)
        finally: 
            conn.close() 

        now = datetime.now(timezone.utc) 
        result = plan_work(sensors, ledger, cfg, now=now) 
        for item in result["work"] : 
            item["planned_at"] = now.isoformat() 
        return result["work"]

    @task(pool=POOL, retries=1, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=15))
    def extract(item) : 
        from vayu.config import build_client, load_config 
        from vayu.extraction import extract_sensor

        cfg = load_config() 
        client = build_client(cfg, Variable.get("OPENAQ_API_KEY")) 
        conn = _pg_conn() 

        try: 
            summary = extract_sensor(item, client, _s3(), conn, cfg, done_since=datetime.fromisoformat(item["planned_at"])) 
        finally: 
            conn.close() 

        if summary["failed_transient"]: 
            raise RuntimeError(f"Transient Failures remain for sensor {item['sensor_id']} : {summary}")

        return summary 

    @task(trigger_rule="all_done") 
    def summarize(work): 

        if work is None: 
            raise RuntimeError("Planning produced no work list; check the plan task's log.")
        if not work: 
            log.info("Nothing to fetch this run: every sensor-month is final")

        since = datetime.fromisoformat(work[0]["planned_at"]) 
        units_to_fetch = sum(len(item["months"]) for item in work)
        conn = _pg_conn() 

        try: 
            with conn.cursor() as cur : 
                cur.execute(
                    """
                        SELECT status, count(*), coalesce(sum(row_count), 0) 
                        FROM extract_ledger 
                        WHERE fetched_at >= %s
                        GROUP BY status 
                        ORDER BY status
                    """
                , (since,))

                by_status = {status: (n,rows) for status,n,rows in cur.fetchall()}
                cur.execute(
                    """
                    SELECT sensor_id, period_month, status, error_message 
                    FROM extract_ledger
                    WHERE fetched_at >= %s AND status <> 'success'
                    ORDER BY sensor_id, period_month
                    """
                , (since,))
                problems = cur.fetchall()
            
        finally: 
            conn.close()

        attempted = sum(n for n,_ in by_status.values()) 
        # log.info("Plan decisions: %s", decisions) 
        log.info("Planned %d sensor-months; attempted %d", units_to_fetch, attempted) 
        for status, (n, rows) in by_status.items() : 
            log.info(" %s: %d sensor-months, %d readings", status, n, rows)
        for sensor_id, month, status, error in problems: 
            log.warning(" %s %s %s: %s", sensor_id, month, status, error)

        transient = by_status.get("failed_transient", (0,0))[0] 
        not_attempted = units_to_fetch - attempted 
        if transient or not_attempted > 0 : 
            raise RuntimeError(
                f"Incomplete run: {transient} transient failures, {not_attempted} sensor_months "
                f"not attempted. They will be retried automatically on the next run."
            )

    schema = ensure_schema() 
    sensors = discover() 
    schema >> sensors 

    work = plan(sensors) 
    extracted = extract.expand(item=work)
    extracted >> summarize(work)

vayu_extract()
