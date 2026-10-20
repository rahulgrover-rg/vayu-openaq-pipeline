from pyspark.sql import Window, functions as F 
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

def _per_parameter(values_by_name, cfg) : 
    ids = {p["name"]: p["id"] for p in cfg["parameters"]}
    expr = F.lit(None).cast("double") 
    for name, value in (values_by_name or {}).items(): 
        if name not in ids : 
            raise ValueError(f"quality rule names unknown parameter '{name}'; known: {sorted(ids)}")
        expr = F.when(F.col("parameter_id") == ids[name], F.lit(float(value))).otherwise(expr)

    return expr 

def deduplicate(readings) : 
    keyed = readings.filter(F.col("period_start_utc").isNotNull())
    unkeyed = readings.filter(F.col("period_start_utc").isNull()) 

    newest_first = Window.partitionBy("sensor_id", "period_start_utc").orderBy(F.col("fetched_at").desc())
    ranked = keyed.withColumn("_rank", F.row_number().over(newest_first))
    unique = ranked.filter("_rank = 1").drop("_rank").unionByName(unkeyed)
    duplicates = ranked.filter("_rank > 1").drop("_rank") 
    return unique, duplicates 


def validate(readings, cfg) : 
    q = cfg["quality"]
    soft = q["soft_flags"]
    sentinels = [float(v) for v in q["sentinel_values"]] 
    sub_sentinel = float(q["sub_reading_sentinel"])
    min_pct = float(soft["min_percent_complete"])
    flag_source = bool(soft["flag_source_flagged"])
    limit = _per_parameter(q["hard_limits"], cfg)
    ceiling = _per_parameter(q.get("instrument_ceilings"), cfg) 
    value = F.col("value") 

    reject_reason = (
        F.when(value.isNull(), "null_value") 
        .when(F.col("period_start_utc").isNull(), "invalid_time")
        .when(F.col("reading_parameter_id") != F.col("parameter_id"), "parameter_mismatch")
        .when(value.isin(sentinels), "sentinel_value")
        .when((F.col("min_in_hour") <= -sub_sentinel) | (F.col("max_in_hour") >= sub_sentinel), "sentinel_in_average") 
        .when(value < 0, "negative_value")
        .when(limit.isNotNull() & (value>limit), "above_instrument_range")
    )

    quality_flags = F.array_compact(F.array(
        F.when(F.col("pct_complete") < min_pct, F.lit("low_coverage")),
        F.when(F.lit(flag_source) & F.col("has_flags"), F.lit("source_flagged")),
        F.when(ceiling.isNotNull() & (F.col("max_in_hour") >= ceiling), F.lit("at_range_ceiling")),
    ))

    return (readings
            .withColumn("reject_reason", reject_reason)
            .withColumn("quality_flags", quality_flags)
    )

def add_local_time(df, timezone) : 
    local = F.from_utc_timestamp(F.col("period_start_utc"), timezone) 
    return (
        df.withColumn("period_start_local", local)
        .withColumn("date_local", F.to_date(local))
        .withColumn("hour_local", F.hour(local)) 
        .withColumn("year", F.year(local))
        .withColumn("month", F.month(local))
    )

def clean(raw, cfg) : 
    readings = flatten_readings(raw) 
    unique,duplicates = deduplicate(readings) 
    checked = add_local_time(validate(unique, cfg), cfg["scope"]["timezone"])
    return {
        "readings" : readings ,
        "duplicates" : duplicates ,
        "curated" : checked.filter(F.col("reject_reason").isNull()).drop("reject_reason"),
        "rejected" : checked.filter(F.col("reject_reason").isNotNull()) 
    }

def write_outputs(out, cfg): 
    st = cfg["storage"] 
    curated_path = f"s3a://{st["curated_bucket"]}/openaq/hours"
    rejected_path = f"s3a://{st["rejected_bucket"]}/openaq/hours" 

    (out["curated"]
     .repartition("year", "month")
     .sortWithinPartitions("sensor_id", "period_start_utc")
     .write.mode("overwrite")
     .partitionBy("year", "month")
     .parquet(curated_path))

    (
        out["rejected"]
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(rejected_path)
    )

    return {"curated_path": curated_path, "rejected_path": rejected_path}

def run_transform(spark, cfg): 
    out = clean(read_raw(spark, cfg["storage"]["raw_bucket"]), cfg) 
    for name in out : 
        out[name] = out[name].cache()

    counts = {name: df.count() for name,df in out.items()} 
    accounted = counts["curated"] + counts["rejected"] + counts["duplicates"] 
    if counts["readings"] != accounted:
        raise RuntimeError(f"Reconciliation failed; nothing written: {counts}") 

    by_reason = {row["reject_reason"]: row["count"]
                 for row in out["rejected"].groupBy("reject_reason").count().collect()}

    paths = write_outputs(out, cfg) 

    written = {"curated": spark.read.parquet(paths["curated_path"]).count(),
               "rejected": spark.read.parquet(paths["rejected_path"]).count()} 

    expected = {"curated": counts["curated"], "rejected": counts["rejected"]}

    if written != expected : 
        raise RuntimeError(f"Write check failed: expected : {expected}, found : {written}")

    return {**counts, "rejected_by_reason": by_reason}