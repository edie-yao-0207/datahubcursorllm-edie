# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 0)

# Query this many days before the start_date to get the previous value
# for state based object stats which don't change often
prev_value_lookback_days = 7

# COMMAND ----------

query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

firmware_trips_df = spark.sql(
    """
 select
    a.date,
    a.time,
    a.org_id,
    a.object_id,
    named_struct("int_value",
      a.value.int_value,
      "proto_value", null,
      "is_databreak", a.value.is_databreak) as value
  from kinesisstats.osdfirmwaretripstate as a
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where a.date >= date_sub('{}', {})
    and a.date <= '{}'
    and a.time <= hwm.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

vg_cm_assoc_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id,
    cm_device_id
  from dataprep_safety.cm_device_health_daily
   where date >= date_sub('{}', {})
    and date <= '{}'
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

cpu_usage_df = spark.sql(
    """
select
    a.date,
    a.time,
    a.org_id,
    a.object_id as cm_device_id,
    a.value.proto_value.cm3x_system_stats.total_cpu_util
from kinesisstats.osdcm3xsystemstats as a
join dataprep_firmware.data_ingestion_high_water_mark as hwm
where a.date >= '{}'
  and a.date <= '{}'
  and a.time <= hwm.time_ms
""".format(
        start_date, end_date
    )
)

firmware_trips_daily_intervals_df = create_intervals_daily(
    create_intervals(firmware_trips_df, lambda x: x > 0, query_end_ms),
    start_date,
    end_date,
)

cm_firmware_trips_df = (
    firmware_trips_daily_intervals_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .select("a.org_id", "b.cm_device_id", "a.start_ms", "a.end_ms")
)

cm_cpu_usage_on_trip_df = (
    cm_firmware_trips_df.alias("a")
    .join(
        cpu_usage_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.cm_device_id") == F.col("b.cm_device_id"))
        & (F.col("b.time") >= F.col("a.start_ms"))
        & (F.col("b.time") <= F.col("a.end_ms")),
    )
    .select("b.date", "a.org_id", "b.cm_device_id", "b.total_cpu_util")
)

device_daily_cpu_usage_df = cm_cpu_usage_on_trip_df.groupBy(
    "date", "org_id", "cm_device_id"
).agg(F.mean(F.col("total_cpu_util")).alias("avg_cpu_usage_on_trip"))

create_or_update_table(
    "dataprep_firmware.cm_device_daily_avg_cpu_usage_on_trip",
    device_daily_cpu_usage_df,
    "date",
    ["date", "org_id", "cm_device_id"],
)
