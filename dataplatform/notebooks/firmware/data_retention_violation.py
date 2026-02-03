# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 0)

prev_value_lookback_days = 7

# COMMAND ----------

raw_retain_duration_df = spark.sql(
    """
    select
        a.date as record_date,
        a.time as updated_at_ms,
        a.org_id,
        a.object_id as device_id,
        a.time - a.value.proto_value.dashcam_stream_status.last_timestamp_deleted_ms as high_res_retain_ms,
        a.time - a.value.proto_value.dashcam_stream_status.last_low_res_timestamp_deleted_ms as low_res_retain_ms
    from kinesisstats.osddashcamstreamstatus as a
    join dataprep_firmware.data_ingestion_high_water_mark as b
    where a.date >= date_sub('{}', {})
      and a.date <= '{}'
      and a.time <= b.time_ms
      """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

# COMMAND ----------

# Construct dates dimension.
dates_df = spark.range(0, 1).select(
    F.explode(
        F.sequence(
            F.to_date(F.lit(start_date)),
            F.to_date(F.lit(end_date)),
            F.expr("interval 1 day"),
        )
    ).alias("date")
)

# Get the latest retention record for each device per date.
exploded = dates_df.alias("d").join(
    raw_retain_duration_df.alias("r"),
    on=(F.col("r.record_date") <= F.col("d.date")),
    how="left",
)

retain_duration_df = (
    exploded.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("date", "device_id").orderBy(F.desc("updated_at_ms"))
        ),
    )
    .filter("rn = 1")
    .drop("rn", "record_date")
)

# COMMAND ----------

MS_PER_DAY = 1000 * 3600 * 24

cfg = (
    spark.table("retentiondb_shards.retention_config")
    .filter(F.col("data_type") == 4)
    .select(
        F.col("org_id"),
        F.col("retain_days").alias("retain_days_cfg"),
        F.col("updated_at_ms").alias("cfg_updated_at_ms"),
    )
    .alias("c")
)

# Join org config updated before record time to calculate retention policy violation.
retention_violation_df = (
    retain_duration_df.alias("d")
    .join(
        cfg,
        on=(F.col("d.org_id") == F.col("c.org_id"))
        & (F.col("d.updated_at_ms") >= F.col("c.cfg_updated_at_ms")),
        how="left",
    )
    .select(
        F.col("d.date"),
        F.col("d.org_id"),
        F.col("d.device_id"),
        F.col("d.updated_at_ms"),
        F.col("d.high_res_retain_ms"),
        F.col("d.low_res_retain_ms"),
        F.col("c.retain_days_cfg"),
        F.col("c.cfg_updated_at_ms"),
    )
    .filter(
        (F.col("high_res_retain_ms") / MS_PER_DAY > F.col("retain_days_cfg"))
        | (F.col("low_res_retain_ms") / MS_PER_DAY > F.col("retain_days_cfg"))
    )
    .withColumn(
        "high_res_excess_days",
        F.greatest(
            F.col("high_res_retain_ms") / MS_PER_DAY - F.col("retain_days_cfg"),
            F.lit(0),
        ),
    )
    .withColumn(
        "low_res_excess_days",
        F.greatest(
            F.col("low_res_retain_ms") / MS_PER_DAY - F.col("retain_days_cfg"), F.lit(0)
        ),
    )
)

# COMMAND ----------

create_or_update_table(
    "dataprep_firmware.data_retention_violation",
    retention_violation_df,
    "date",
    ["date", "org_id", "device_id"],
)
