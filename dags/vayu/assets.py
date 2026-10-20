from airflow.sdk import Asset 

RAW_OPENAQ_HOURS = Asset(name="raw_openaq_hours", uri="s3://raw/openaq/hours")