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

# Get list of devices with tile based rolling stops enabled via config

tile_based_rolling_stop_daily_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.tile_rolling_stop.stop_sign_policy.enabled) is true then 1
        else 0
      end,
      "proto_value", null,
      "is_databreak", false) as value
  from s3bigstats.osdreporteddeviceconfig_raw
  where date >= date_sub('{}', {})
    and date <= '{}'
  group by
    date,
    time,
    org_id,
    object_id
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

tile_based_rolling_stop_daily_enabled_df = days_enabled_for_full_day(
    tile_based_rolling_stop_daily_enabled_cfg_df
)


# Join with trip data to get active status

cm_device_health_daily_df = spark.sql(
    """
   select
      *
  from dataprep_safety.cm_device_health_daily
  where date >= date_sub('{}', {})
    and date <= '{}'
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

tile_based_rolling_stop_daily_devices_df = (
    tile_based_rolling_stop_daily_enabled_df.alias("a")
    .join(
        cm_device_health_daily_df.alias("b"),
        (
            (F.col("a.date") == F.col("b.date"))
            & (F.col("a.org_id") == F.col("b.org_id"))
            & (F.col("a.device_id") == F.col("b.cm_device_id"))
        ),
        how="inner",
    )
    .groupBy("a.date", "a.org_id", "a.device_id")
    .agg(
        F.max(F.col("b.total_trips")).alias("trip_count"),
        F.max(F.col("b.total_distance_meters") / 1609).alias("trip_miles"),
    )
    .withColumn("active", F.col("trip_count") > 0)
)

# COMMAND ----------

# Update Devices Table

create_or_update_table(
    "dataprep_firmware.tile_based_rolling_stop_daily_devices",
    tile_based_rolling_stop_daily_devices_df,
    "date",
    ["date", "org_id", "device_id"],
)


# COMMAND ----------

# Create Active Device View

spark.sql(
    """
    create or replace view dataprep_firmware.tile_based_rolling_stop_daily_active_devices as (
        select * from dataprep_firmware.tile_based_rolling_stop_daily_devices where active is true
    )
"""
)

# COMMAND ----------

# Build Stop Locations Processed Count Table

stop_locations_processed_count_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    metric.tags as metric_tags,
    coalesce(sum(metric.value), 0) as count
  from
    kinesisstats.osdfirmwaremetrics
    lateral view explode(value.proto_value.firmware_metrics.metrics) as metric
  where date between '{}' and '{}'
    and metric.metric_type = 1 -- 1: COUNT
    and metric.name = "rolling_stop.stop_location.processed"
  group by
    date,
    org_id,
    object_id,
    metric.tags
  """.format(
        start_date, end_date
    )
)

# Extract key-value pairs from tags
stop_locations_processed_count_df = (
    stop_locations_processed_count_df.withColumn(
        "location_type", F.split(F.col("metric_tags").getItem(0), ":").getItem(1)
    )  # ex: location_type:StopSign
    .withColumn(
        "result", F.split(F.col("metric_tags").getItem(1), ":").getItem(1)
    )  # ex: result:PROCESSED_STOP
    .drop(F.col("metric_tags"))
    .groupBy("date", "org_id", "device_id", "location_type", "result")
    .agg(F.sum("count").alias("count"))
    .fillna(0)
)

# COMMAND ----------

# Update Stop Locations Processed Count Table

create_or_update_table(
    "dataprep_firmware.tile_based_rolling_stop_stop_locations_processed_count",
    stop_locations_processed_count_df,
    "date",
    ["date", "org_id", "device_id", "location_type", "result"],
)

# COMMAND ----------

# Build Rolling Stop Sign Event Count Table

rolling_stop_sign_event_count_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    concat_ws("-", value.proto_value.accelerometer_event.tile_rolling_stop_sign_metadata.current_tile_info.version, value.proto_value.accelerometer_event.tile_rolling_stop_sign_metadata.current_tile_info.tiles_cache_key) as tile_version_and_cache_key,
    count(*) as count
  from kinesisstats.osdaccelerometer
  where date between '{}' and '{}'
    and value.proto_value.accelerometer_event.harsh_accel_type = 17 -- haTileRollingStopSign
  group by
    date,
    org_id,
    device_id,
    tile_version_and_cache_key
""".format(
        start_date, end_date
    )
).fillna(0)

# COMMAND ----------

# Update Rolling Stop Sign Event Count Table

create_or_update_table(
    "dataprep_firmware.tile_based_rolling_stop_rolling_stop_sign_event_count",
    rolling_stop_sign_event_count_df,
    "date",
    ["date", "org_id", "device_id", "tile_version_and_cache_key"],
)
