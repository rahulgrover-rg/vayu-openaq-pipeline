from datetime import datetime 
from airflow.sdk import dag, task 

@dag(
    schedule=None, 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smoke_test"],
)
def smoke_test():

    @task
    def check_postgres():
        from airflow.providers.postgres.hooks.postgres import PostgresHook 
        hook = PostgresHook(postgres_conn_id="pipeline_db")
        db_name = hook.get_first("SELECT current_database();")[0]
        print(f"Connected to Postgres database: {db_name}")

    @task 
    def check_minio():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
        client = S3Hook(aws_conn_id="minio").get_conn()
        buckets = [b["Name"] for b in client.list_buckets()["Buckets"]]
        print(f"Connected to MinIO. Buckets: {buckets}")

    check_postgres() >> check_minio()

smoke_test()