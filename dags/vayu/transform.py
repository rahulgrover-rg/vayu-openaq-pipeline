from pyspark.sql import functions as F 
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

_TIMESTAMP_PAIR = StructType([
    StructField("utc", StringType()),
    StructField("local", StringType()),
])

READING_SCHEMA = StructType([
    StructField("value", DoubleType()),
    StructField("flagInfo", StructType([StructField("hasFlags", BooleanType())])),
    StructField("parameter", StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("units", StringType()),
    ])),
    StructField("period", StructType([
        StructField("label", StringType()),
        StructField("datetimeFrom", _TIMESTAMP_PAIR),
        StructField("datetimeTo", _TIMESTAMP_PAIR),
    ])),
    StructField("coverage", StructType([
        StructField("expectedCount", LongType()),
        StructField("observedCount", LongType()),
        StructField("percentComplete", DoubleType()),
    ])),
    StructField("summary", StructType([
        StructField("min", DoubleType()),
        StructField("max", DoubleType()),
    ]))
])

ENVELOPE_SCHEMA = StructType([
    StructField("source", StringType()),
    StructField("sensor_id", LongType()), 
    StructField("location_id", LongType()),
    StructField("parameter_id", LongType()),
    StructField("period_month", StringType()),
    StructField("fetched_at", StringType()),
    StructField("row_count", LongType()), 
    StructField("results", ArrayType(READING_SCHEMA)),
])

def read_raw(spark, bucket, prefix="openaq/hours"): 
    return (
        spark.read
        .schema(ENVELOPE_SCHEMA)
        .option("multiLine", "true") 
        .json(f"s3a://{bucket}/{prefix}")
        .withColumn("source_file", F.col("_metadata.file_path"))
    )

def flatten_readings(raw) : 
    r = F.col("r") 
    return raw.select(
        "sensor_id", 
        "location_id", 
        "parameter_id",
        "fetched_at",
        "source_file",
        F.explode("results").alias("r"),
    ).select(
        "sensor_id",
        "location_id",
        "parameter_id",
        F.try_to_timestamp(r["period"]["datetimeFrom"]["utc"]).alias("period_start_utc"),
        F.try_to_timestamp(r["period"]["datetimeTo"]["utc"]).alias("period_end_utc"),
        r["value"].alias("value"),
        r["parameter"]["id"].alias("reading_parameter_id"),
        r["parameter"]["units"].alias("units"),
        r["coverage"]["percentComplete"].alias("pct_complete"),
        r["coverage"]["observedCount"].alias("observed_count"),
        r["coverage"]["expectedCount"].alias("expected_count"),
        r["summary"]["min"].alias("min_in_hour"),
        r["summary"]["max"].alias("max_in_hour"),
        r["flagInfo"]["hasFlags"].alias("has_flags"),
        F.try_to_timestamp(F.col("fetched_at")).alias("fetched_at"),
        "source_file",
    )