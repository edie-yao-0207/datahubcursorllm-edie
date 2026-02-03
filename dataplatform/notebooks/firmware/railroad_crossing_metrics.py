# COMMAND ----------

# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# Query this many days before the start_date to get the previous value
# for state based object stats which don't change often
prev_value_lookback_days = 7

query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------


def days_enabled_for_full_day(stat_df):
    stat_intervals_daily_df = create_intervals_daily(
        create_intervals(stat_df, lambda x: x > 0, query_end_ms),
        start_date,
        end_date,
    )
    stat_full_days_df = stat_intervals_daily_df.filter(
        F.col("end_ms") - F.col("start_ms") == day_ms() - 1
    ).select("date", "org_id", "device_id")
    return stat_full_days_df


# COMMAND ----------

# Build Devices Table

# Get list of devices with railroad crossing enabled via config

railroad_crossing_daily_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.tile_rolling_stop.railroad_crossing_policy.enabled) is true then 1
        else 0
      end,
      "proto_value", null,
      "is_databreak", false) as value
  from s3bigstats.osdreporteddeviceconfig_raw
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where date >= date_sub('{}', {})
    and date <= '{}'
    and time <= hwm.time_ms
  group by
    date,
    time,
    org_id,
    object_id
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

railroad_crossing_daily_enabled_df = days_enabled_for_full_day(
    railroad_crossing_daily_enabled_cfg_df
)


# Join with trip data to get active status

cm_octo_vg_daily_associations_unique_df = spark.sql(
    """
   select
      *
  from dataprep_firmware.cm_octo_vg_daily_associations_unique
  where date >= date_sub('{}', {})
    and date <= '{}'
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

fct_trips_daily_df = spark.sql(
    """
   select
      *
  from datamodel_telematics.fct_trips_daily
  where date >= date_sub('{}', {})
    and date <= '{}'
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

railroad_crossing_daily_devices_df = (
    railroad_crossing_daily_enabled_df.alias("a")
    .join(
        cm_octo_vg_daily_associations_unique_df.alias("b"),
        (
            (F.col("a.date") == F.col("b.date"))
            & (F.col("a.device_id") == F.col("b.cm_device_id"))
        ),
        how="inner",
    )
    .join(
        fct_trips_daily_df.alias("c"),
        (
            (F.col("a.date") == F.col("c.date"))
            & (F.col("a.org_id") == F.col("c.org_id"))
            & (F.col("b.vg_device_id") == F.col("c.device_id"))
        ),
        how="outer",
    )
    .groupBy("a.date", "a.org_id", "a.device_id")
    .agg(
        F.max(F.col("c.trip_count")).alias("trip_count"),
        F.max(F.col("c.total_distance_miles")).alias("total_distance_miles"),
    )
    .withColumn("active", F.col("trip_count") > 0)
)

# COMMAND ----------

# Update Devices Table

create_or_update_table(
    "dataprep_firmware.railroad_crossing_daily_devices",
    railroad_crossing_daily_devices_df,
    "date",
    ["date", "org_id", "device_id"],
)


# COMMAND ----------

# Create Active Device View

spark.sql(
    """
    create or replace view dataprep_firmware.railroad_crossing_daily_active_devices as (
        select * from dataprep_firmware.railroad_crossing_daily_devices where active is true
    )
"""
)

# COMMAND ----------

# Build Railroad Crossing Locations Processed Count Table

from pyspark.sql.types import MapType, StringType

# Helper for parsing tags array into map
def parse_tags_array(arr):
    if arr is None:
        return {}
    return {kv.split(":", 1)[0]: kv.split(":", 1)[1] for kv in arr if ":" in kv}


spark.udf.register(
    "parse_tags_array", parse_tags_array, MapType(StringType(), StringType())
)


railroad_crossing_locations_processed_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id,
    parse_tags_array(tags)['location_type'] as location_type,
    parse_tags_array(tags)['result'] as result,
    parse_tags_array(tags)['layer_source'] as layer_source,
    value
  from
    product_analytics_staging.fct_osdfirmwaremetrics
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where date between '{}' and '{}'
    and time <= hwm.time_ms
    and name = "rolling_stop.stop_location.processed"
""".format(
        start_date, end_date
    )
)

railroad_crossing_locations_processed_count_df = (
    railroad_crossing_locations_processed_df.filter(
        F.col("location_type") == "RailroadCrossing"
    )
    .groupBy("date", "org_id", "device_id", "result", "location_type", "layer_source")
    .agg(F.sum("value").alias("count"))
    .fillna(0)
)

# COMMAND ----------

# Update Railroad Crossing Locations Processed Count Table

create_or_update_table(
    "dataprep_firmware.railroad_crossing_locations_processed_count",
    railroad_crossing_locations_processed_count_df,
    "date",
    ["date", "org_id", "device_id", "location_type", "result", "layer_source"],
)

# COMMAND ----------

# Build Railroad Crossing Events Table

from pyspark.sql.types import MapType, StringType

# Helper for extracting desired tile layer version
def extract_tile_layer_version(layers, layer_name):
    if layers is None:
        return None
    for layer in layers:
        if "layer_name" in layer:
            if layer["layer_name"] == layer_name:
                if "version" in layer:
                    return layer["version"]
    return None


spark.udf.register(
    "extract_tile_layer_version", extract_tile_layer_version, StringType()
)

railroad_crossing_events_df = spark.sql(
    """
  select
    date,
    time,
    org_id,
    object_id as device_id,
    extract_tile_layer_version(value.proto_value.accelerometer_event.tile_rolling_railroad_crossing_metadata.current_tile_info.layers, value.proto_value.accelerometer_event.tile_rolling_railroad_crossing_metadata.stop_location_layer_source) as stop_location_layer_source_version,
    value.proto_value.accelerometer_event.tile_rolling_railroad_crossing_metadata.stop_location_layer_source,
    case
      when value.proto_value.accelerometer_event.tile_rolling_railroad_crossing_metadata.process_result = 2 then "ROLLED_STOP"
      when value.proto_value.accelerometer_event.tile_rolling_railroad_crossing_metadata.process_result = 3 then "PROCESSED_STOP"
      when value.proto_value.accelerometer_event.tile_rolling_railroad_crossing_metadata.process_result = 4 then "IGNORED_STOP"
      else "UNKNOWN"
    end as process_result
  from kinesisstats.osdaccelerometer
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where date between '{}' and '{}'
    and time <= hwm.time_ms
    and value.proto_value.accelerometer_event.harsh_accel_type = 18 -- haTileRollingRailroadCrossing
""".format(
        start_date, end_date
    )
)

# COMMAND ----------

# Update Railroad Crossing Events Table

create_or_update_table(
    "dataprep_firmware.railroad_crossing_events",
    railroad_crossing_events_df,
    "date",
    ["date", "time", "org_id", "device_id"],
)

# COMMAND ----------

# Build Railroad Crossing Event Count Table

railroad_crossing_event_count_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    count(*) as count
  from kinesisstats.osdaccelerometer
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where date between '{}' and '{}'
    and time <= hwm.time_ms
    and value.proto_value.accelerometer_event.harsh_accel_type = 18 -- haTileRollingRailroadCrossing
  group by
    date,
    org_id,
    object_id
""".format(
        start_date, end_date
    )
).fillna(0)

# COMMAND ----------

# Update Railroad Crossing Event Count Table

create_or_update_table(
    "dataprep_firmware.railroad_crossing_event_count",
    railroad_crossing_event_count_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Build Railroad Crossing Audio Alert Count Table

railroad_crossing_audio_alert_count_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    count(*) as count
  from kinesisstats.osdcm3xaudioalertinfo
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where date between '{}' and '{}'
    and time <= hwm.time_ms
    and value.proto_value.cm3x_audio_alert_info.event_type = 8 -- RAILROAD_CROSSING_EVENT
  group by
    date,
    org_id,
    object_id
""".format(
        start_date, end_date
    )
)

# COMMAND ----------

# Update Railroad Crossing Audio Alert Count Table

create_or_update_table(
    "dataprep_firmware.railroad_crossing_audio_alert_count",
    railroad_crossing_audio_alert_count_df,
    "date",
    ["date", "org_id", "device_id"],
)
