from datetime import timedelta 
import logging
import pendulum 
from airflow.sdk import dag, task 
from vayu.assets import RAW_OPENAQ_HOURS 

log = logging.getLogger(__name__)

@dag(
    dag_id = "vayu_transform",
    schedule = [RAW_OPENAQ_HOURS],
    start_date = pendulum.datetime(2026,10,1,tz="Asia/Kolkata"),
    catchup = False,
    max_active_runs = 1,
    tags = ["vayu", "transform"]
)
def vayu_transform(): 

    @task(execution_timeout=timedelta(minutes=30))
    def spark_transform(): 
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
        from vayu.config import load_config 
        from vayu.spark_session import build_spark 
        from vayu.transform import run_transform 

        cfg = load_config() 
        hook = S3Hook(aws_conn_id = "minio")
        creds = hook.get_credentials()

        spark = build_spark(endpoint=hook.conn_config.endpoint_url, access_key=creds.access_key, secret_key=creds.secret_key, app_name="vayu-transform")

        spark.sparkContext.setLogLevel("WARN") 

        try: 
            return run_transform(spark,cfg) 
        finally: 
            spark.stop()

    @task
    def dbt_source_freshness():
        from vayu.dbt_runner import FRESHNESS_RESULTS, run_dbt, summarize_freshness

        exit_code = run_dbt(["source", "freshness"], FRESHNESS_RESULTS)
        summary = summarize_freshness()
        for s in summary: 
            if s["status"] != "pass" : 
                log.warning("Freshness %s: %s newest row is %s h old (%s)", s["status"].upper(),s["source"], s["age_hours"], s["max_loaded_at"])

        if any(s["status"] in ("error", "runtime error") for s in summary) : 
            raise RuntimeError(f"dbt source freshness failed (exit {exit_code}) : {summary}")
        return summary 

    @task(execution_timeout=timedelta(minutes=30))
    def dbt_build(): 
        from vayu.dbt_runner import RUN_RESULTS, run_dbt, summarize_build
        
        exit_code = run_dbt(["build"],RUN_RESULTS) 
        summary = summarize_build() 
        for p in summary["problems"]: 
            log.warning("%s: %s (%s)", p["status"].upper(), p["node"], p["message"])

        if exit_code != 0: 
            raise RuntimeError(f"dbt build failed (exit {exit_code}) : {summary["statuses"]}")

        return summary

    spark_transform() >> dbt_source_freshness() >> dbt_build()

vayu_transform()