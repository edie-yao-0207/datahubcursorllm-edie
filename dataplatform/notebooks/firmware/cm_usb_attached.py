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

cm_usb_attached_df = spark.sql(
    """
 select
    a.date,
    a.time,
    a.org_id,
    b.cm_device_id as object_id,
    named_struct("int_value",
    case
      when exists(a.value.proto_value.attached_usb_devices.usb_id, x -> x == 1668105037) = false and exists(a.value.proto_value.attached_usb_devices.usb_id, x -> x == 185272842) = false then 0
      else 1
    end,
    "proto_value", null,
    "is_databreak", a.value.is_databreak) as value
  from kinesisstats.osdattachedusbdevices as a
  join dataprep_safety.cm_device_health_daily as b
    on a.object_id = b.device_id
    and a.org_id = b.org_id
    and a.date = b.date
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where a.date >= date_sub('{}', {})
    and a.date <= '{}'
    and a.time <= hwm.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

osdpowerstate_df = spark.sql(
    """
select *
from kinesisstats.osdpowerstate as a
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

cm_usb_attached_intervals_daily_df = create_intervals_daily(
    create_intervals(cm_usb_attached_df, lambda x: x > 0, query_end_ms),
    start_date,
    end_date,
)
power_state_intervals_daily_df = (
    create_intervals_daily(
        create_intervals(osdpowerstate_df, lambda x: x not in (2, 10), query_end_ms),
        start_date,
        end_date,
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

cm_usb_while_power_daily_df = intersect_intervals(
    cm_usb_attached_intervals_daily_df, power_state_intervals_daily_df
)

cm_usb_agg_df = cm_usb_while_power_daily_df.groupBy("date", "org_id", "device_id").agg(
    F.sum(F.col("end_ms") - F.col("start_ms")).alias("attached_ms")
)
power_state_agg_df = power_state_intervals_daily_df.groupBy(
    "date", "org_id", "device_id"
).agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("total_ms"))

joined_df = power_state_agg_df.join(
    cm_usb_agg_df, ["date", "org_id", "device_id"], how="left"
).select(
    "date",
    "org_id",
    "device_id",
    "total_ms",
    F.coalesce("attached_ms").alias("attached_ms"),
)

create_or_update_table(
    "dataprep_firmware.cm_device_daily_usb_enumerates",
    joined_df,
    "date",
    ["date", "org_id", "device_id"],
)
