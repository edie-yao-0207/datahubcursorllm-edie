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

osdpowerstate_df = spark.sql(
    """
select *
from kinesisstats.osdpowerstate as a
join dataprep_firmware.data_ingestion_high_water_mark as b
where date >= date_sub('{}', {})
  and date <= '{}'
  and a.time <= b.time_ms
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

hb_df = spark.sql(
    """
  select * from dataprep.device_heartbeats_extended
"""
)

vg_non_full_power_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x not in (1, 7), query_end_ms
)
vg_non_full_power_intervals_terminated_df = terminate_intervals_at_last_hb(
    vg_non_full_power_intervals_df, hb_df
)
vg_daily_df = (
    create_intervals_daily(
        vg_non_full_power_intervals_terminated_df, start_date, end_date
    )
    .alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id")),
    )
    .select(
        "a.date",
        "a.org_id",
        F.col("b.cm_device_id").alias("device_id"),
        "a.start_ms",
        "a.end_ms",
    )
)

cm_full_power_intervals_df = create_intervals(
    osdpowerstate_df, lambda x: x == 1, query_end_ms
)
cm_full_power_terminated_df = terminate_intervals_at_last_hb(
    cm_full_power_intervals_df, hb_df
)
cm_daily_df = create_intervals_daily(cm_full_power_terminated_df, start_date, end_date)

cm_full_power_intervals_during_while_not_vg_full_power_daily_df = intersect_intervals(
    cm_daily_df,
    vg_daily_df,
)

joined_df = (
    vg_daily_df.alias("a")
    .join(
        cm_full_power_intervals_during_while_not_vg_full_power_daily_df.alias("b"),
        ["date", "org_id", "device_id"],
        "left",
    )
    .groupBy("date", "org_id", "device_id")
    .agg(
        F.sum(F.col("a.end_ms") - F.col("a.start_ms")).alias("vg_non_full_power_ms"),
        F.sum(F.col("b.end_ms") - F.col("b.start_ms")).alias(
            "cm_full_power_while_vg_non_full_power_ms"
        ),
    )
)

create_or_update_table(
    "dataprep_firmware.cm_device_daily_power",
    joined_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------
