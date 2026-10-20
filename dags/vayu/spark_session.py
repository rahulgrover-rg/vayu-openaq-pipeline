import glob 
import os 
import re 

def bundled_hadoop_version() : 
    import pyspark 
    pattern = os.path.join(os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api-*.jar")
    matches = glob.glob(pattern) 
    if not matches : 
        raise RuntimeError(f"No hadoop-client-api jar found at pattern {pattern}") 

    return re.search(r"hadoop-client-api-(.*)\.jar$", os.path.basename(matches[0])).group(1)

def build_spark(endpoint, access_key, secret_key, app_name="vayu", master="local[2]", driver_memory="2g") : 
    from pyspark.sql import SparkSession 
    hadoop = bundled_hadoop_version() 
    return ( 
        SparkSession.builder
        .appName(app_name) 
        .master(master) 
        .config("spark.driver.memory", driver_memory) 
        .config("spark.sql.session.timeZone", "UTC") 
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        # s3a connector 
        .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop}") 
        .config("spark.jars.ivy", "/home/airflow/.ivy2") 
        # MINIO connection
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) 
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") 
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )