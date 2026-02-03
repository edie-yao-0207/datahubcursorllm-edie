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
      when exists(a.value.proto_value.attached_usb_devices.usb_id, x -> x == 1668105037) = false then 0
      else 1
    end,
    "proto_value", null,
    "is_databreak", a.value.is_databreak) as value
  from kinesisstats.osdattachedusbdevices as a
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  join dataprep_firmware.cm_octo_vg_daily_associations_unique as b
    on a.date = b.date
    and a.object_id = b.vg_device_id
  join clouddb.gateways as c
    on b.cm_gateway_id = c.id
  where a.date >= date_sub('{}', {})
    and a.date <= '{}'
    and a.time <= hwm.time_ms
    and c.product_id in (43, 44)
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

brigid_usb_attached_df = spark.sql(
    """
 select
    a.date,
    a.time,
    a.org_id,
    b.cm_device_id as object_id,
    named_struct("int_value",
    case
      when exists(a.value.proto_value.attached_usb_devices.usb_id, x -> x == 185272842) = false then 0
      else 1
    end,
    "proto_value", null,
    "is_databreak", a.value.is_databreak) as value
  from kinesisstats.osdattachedusbdevices as a
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  join dataprep_firmware.cm_octo_vg_daily_associations_unique as b
    on a.date = b.date
    and a.object_id = b.vg_device_id
  join clouddb.gateways as c
    on b.cm_gateway_id = c.id
  where a.date >= date_sub('{}', {})
    and a.date <= '{}'
    and a.time <= hwm.time_ms
    and c.product_id in (155, 167)
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

octo_usb_attached_df = spark.sql(
    """
 select
    a.date,
    a.time,
    a.org_id,
    b.cm_device_id as object_id,
    named_struct("int_value",
    case
      when exists(a.value.proto_value.attached_usb_devices.usb_id, x -> x == 1096968297) = false then 0
      else 1
    end,
    "proto_value", null,
    "is_databreak", a.value.is_databreak) as value
  from kinesisstats.osdattachedusbdevices as a
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  join dataprep_firmware.cm_octo_vg_daily_associations_unique as b
    on a.date = b.date
    and a.object_id = b.vg_device_id
  join clouddb.gateways as c
    on b.cm_gateway_id = c.id
  where a.date >= date_sub('{}', {})
    and a.date <= '{}'
    and a.time <= hwm.time_ms
    and c.product_id in (126)
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

osddashcamconnected_df = spark.sql(
    """
select
    a.date,
    a.time,
    a.org_id,
    b.cm_device_id as object_id,
    named_struct("int_value", value.int_value,
    "proto_value", null,
    "is_databreak", a.value.is_databreak) as value
from kinesisstats.osddashcamconnected as a
join dataprep_firmware.data_ingestion_high_water_mark as hwm
join dataprep_firmware.cm_octo_vg_daily_associations_unique as b
  on a.object_id = b.vg_device_id
  and a.date = b.date
where a.date >= date_sub('{}', {})
  and a.date <= '{}'
  and a.time <= hwm.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

osdmulticamconnected_df = spark.sql(
    """
select
    a.date,
    a.time,
    a.org_id,
    b.cm_device_id as object_id,
    named_struct("int_value", value.int_value,
    "proto_value", null,
    "is_databreak", a.value.is_databreak) as value
from kinesisstats.osdmulticamconnected as a
join dataprep_firmware.data_ingestion_high_water_mark as hwm
join dataprep_firmware.cm_octo_vg_daily_associations_unique as b
  on a.object_id = b.vg_device_id
  and a.date = b.date
where a.date >= date_sub('{}', {})
  and a.date <= '{}'
  and a.time <= hwm.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

all_cm_usb_attached_df = cm_usb_attached_df.unionAll(brigid_usb_attached_df).unionAll(
    octo_usb_attached_df
)

all_camera_connected_df = osddashcamconnected_df.unionAll(osdmulticamconnected_df)

cm_usb_attached_intervals_df = create_intervals(
    all_cm_usb_attached_df, lambda x: x > 0, query_end_ms
)

dashcam_intervals_df = create_intervals(
    all_camera_connected_df, lambda x: x == 1, query_end_ms
)
dashcam_while_usb_df = intersect_intervals(
    cm_usb_attached_intervals_df, dashcam_intervals_df
)

cm_usb_attached_intervals_daily_df = create_intervals_daily(
    cm_usb_attached_intervals_df, start_date, end_date
)
dashcam_while_usb_daily_df = create_intervals_daily(
    dashcam_while_usb_df, start_date, end_date
)

dashcam_agg_df = dashcam_while_usb_daily_df.groupBy("date", "org_id", "device_id").agg(
    F.sum(F.col("end_ms") - F.col("start_ms")).alias("dashcam_connected_ms")
)
usb_agg_df = cm_usb_attached_intervals_daily_df.groupBy(
    "date", "org_id", "device_id"
).agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("total_ms"))


joined_df = dashcam_agg_df.join(
    usb_agg_df, ["date", "org_id", "device_id"], how="left"
).select(
    "date",
    "org_id",
    "device_id",
    "total_ms",
    F.coalesce("dashcam_connected_ms").alias("dashcam_connected_ms"),
)

create_or_update_table(
    "dataprep_firmware.cm_device_daily_dashcam_vg_connection",
    joined_df,
    "date",
    ["date", "org_id", "device_id"],
)
