from datetime import timedelta 

import pendulum 
from airflow.sdk import dag, task 
from vayu.assets import RAW_OPENAQ_HOURS 

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

    spark_transform()

vayu_transform()