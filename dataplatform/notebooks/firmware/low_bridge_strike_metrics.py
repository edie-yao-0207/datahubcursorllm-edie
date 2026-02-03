# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

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

# Get list of devices with low bridge strike enabled via config

low_bridge_strike_daily_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.low_bridge_strike.enabled) = 1 then 1
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

low_bridge_strike_daily_enabled_df = days_enabled_for_full_day(
    low_bridge_strike_daily_enabled_cfg_df
)


# Get list of devices with low bridge strike shadow mode enabled via config

low_bridge_strike_shadow_mode_daily_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.low_bridge_strike.audio_alerts_in_shadow_mode_enabled) = 1 then 1
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

low_bridge_strike_shadow_mode_daily_enabled_df = days_enabled_for_full_day(
    low_bridge_strike_shadow_mode_daily_enabled_cfg_df
).withColumn("shadow_mode", F.lit(True))


# Join with trip data to get active status and join with shadow mode table

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

low_bridge_strike_daily_devices_df = (
    low_bridge_strike_daily_enabled_df.alias("a")
    .join(
        cm_device_health_daily_df.alias("b"),
        (
            (F.col("a.date") == F.col("b.date"))
            & (F.col("a.org_id") == F.col("b.org_id"))
            & (F.col("a.device_id") == F.col("b.cm_device_id"))
        ),
        how="inner",
    )
    .join(
        low_bridge_strike_shadow_mode_daily_enabled_df.alias("c"),
        (
            (F.col("a.date") == F.col("c.date"))
            & (F.col("a.org_id") == F.col("c.org_id"))
            & (F.col("a.device_id") == F.col("c.device_id"))
        ),
        how="left",
    )
    .groupBy("a.date", "a.org_id", "a.device_id", "c.shadow_mode")
    .agg(
        F.max(F.col("b.total_trips")).alias("trip_count"),
        F.max(F.col("b.total_distance_meters") / 1609).alias("trip_miles"),
    )
    .withColumn("active", F.col("trip_count") > 0)
    .withColumn("shadow_mode", F.coalesce(F.col("c.shadow_mode"), F.lit(False)))
)

# COMMAND ----------

# Update Devices Table

create_or_update_table(
    "dataprep_firmware.low_bridge_strike_daily_devices",
    low_bridge_strike_daily_devices_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Create Active Device View

spark.sql(
    """
    create or replace view dataprep_firmware.low_bridge_strike_daily_active_devices as (
        select * from dataprep_firmware.low_bridge_strike_daily_devices where active is true
    )
"""
)

# COMMAND ----------

# Build Low Bridge Strike Error Count Table

low_bridge_strike_error_count_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    metric.name as metric_name,
    coalesce(sum(metric.value), 0) as count
  from
    kinesisstats.osdfirmwaremetrics
    lateral view explode(value.proto_value.firmware_metrics.metrics) as metric
  where date between '{}' and '{}'
    and metric.metric_type = 1 -- 1: COUNT
    and (metric.name like "low_bridge_strike%")
    and ((metric.name like "%error%") or (metric.name like "%invalid%"))
  group by
    date,
    org_id,
    object_id,
    metric.name
  """.format(
        start_date, end_date
    )
)

# COMMAND ----------

# Update Low Bridge Strike Error Count Table

create_or_update_table(
    "dataprep_firmware.low_bridge_strike_error_count",
    low_bridge_strike_error_count_df,
    "date",
    ["date", "org_id", "device_id", "metric_name"],
)

# COMMAND ----------

# Build Bridge Nodes Processed Count Table

low_bridge_strike_bridge_nodes_processed_count_df = spark.sql(
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
    and metric.name = "low_bridge_strike.bridge_node.processed"
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
low_bridge_strike_bridge_nodes_processed_count_df = (
    low_bridge_strike_bridge_nodes_processed_count_df.withColumn(
        "bridge_height_exceeded",
        F.split(F.col("metric_tags").getItem(0), ":").getItem(1),
    )  # ex: bridge_height_exceeded:true
    .withColumn(
        "ways_away_threshold_triggered",
        F.split(F.col("metric_tags").getItem(1), ":").getItem(1),
    )  # ex: ways_away_threshold_triggered:true
    .withColumn(
        "distance_threshold_triggered",
        F.split(F.col("metric_tags").getItem(2), ":").getItem(1),
    )  # ex: distance_threshold_triggered:true
    .drop(F.col("metric_tags"))
    .groupBy(
        "date",
        "org_id",
        "device_id",
        "bridge_height_exceeded",
        "ways_away_threshold_triggered",
        "distance_threshold_triggered",
    )
    .agg(F.sum("count").alias("count"))
)

# COMMAND ----------

# Update Bridge Nodes Processed Count Table

create_or_update_table(
    "dataprep_firmware.low_bridge_strike_bridge_nodes_processed_count",
    low_bridge_strike_bridge_nodes_processed_count_df,
    "date",
    [
        "date",
        "org_id",
        "device_id",
        "bridge_height_exceeded",
        "ways_away_threshold_triggered",
        "distance_threshold_triggered",
    ],
)

# COMMAND ----------

# Build Low Bridge Strike Warning Events Table

low_bridge_strike_warning_events_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    time as event_ms,
    value.proto_value.low_bridge_strike_warning_event.current_osrm_match.gps_utc_ms,
    value.proto_value.low_bridge_strike_warning_event.current_gps.speed_milli_knot * 0.001852 as gps_speed_kmph,
    coalesce(value.proto_value.low_bridge_strike_warning_event.height_threshold_triggered, false) as height_threshold_triggered,
    coalesce(value.proto_value.low_bridge_strike_warning_event.ways_away_threshold_triggered, false) as ways_away_threshold_triggered,
    coalesce(value.proto_value.low_bridge_strike_warning_event.distance_threshold_triggered, false) as distance_threshold_triggered,
    coalesce(value.proto_value.low_bridge_strike_warning_event.hidden_to_customer, false) as hidden_to_customer,
    value.proto_value.low_bridge_strike_warning_event.current_tile_info.bridge_location_version,
    value.proto_value.low_bridge_strike_warning_event.current_tile_info.bridge_location_cache_key,
    concat_ws("-", value.proto_value.low_bridge_strike_warning_event.current_tile_info.bridge_location_version, value.proto_value.low_bridge_strike_warning_event.current_tile_info.bridge_location_cache_key) as bridge_location_version_and_cache_key,
    value.proto_value.low_bridge_strike_warning_event.distance_to_bridge_node_meters,
    value.proto_value.low_bridge_strike_warning_event.ways_away_from_bridge_way
  from kinesisstats.osdlowbridgestrikewarningevent
  where date between '{}' and '{}'
""".format(
        start_date, end_date
    )
)

low_bridge_strike_warning_event_count_df = low_bridge_strike_warning_events_df.groupBy(
    "date",
    "org_id",
    "device_id",
    "height_threshold_triggered",
    "ways_away_threshold_triggered",
    "distance_threshold_triggered",
    "hidden_to_customer",
    "bridge_location_version",
    "bridge_location_cache_key",
    "bridge_location_version_and_cache_key",
).agg(F.count("*").alias("count"))

# COMMAND ----------

# Update Low Bridge Strike Warning Events Table

create_or_update_table(
    "dataprep_firmware.low_bridge_strike_warning_events",
    low_bridge_strike_warning_events_df,
    "date",
    ["date", "org_id", "device_id", "event_ms"],
)

# COMMAND ----------

# Update Low Bridge Strike Warning Event Count Table

create_or_update_table(
    "dataprep_firmware.low_bridge_strike_warning_event_count",
    low_bridge_strike_warning_event_count_df,
    "date",
    [
        "date",
        "org_id",
        "device_id",
        "height_threshold_triggered",
        "ways_away_threshold_triggered",
        "distance_threshold_triggered",
        "hidden_to_customer",
        "bridge_location_version_and_cache_key",
    ],
)

# COMMAND ----------

# Build Low Bridge Strike Warning Audio Alerts Table

low_bridge_strike_warning_audio_alerts_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    coalesce(value.proto_value.cm3x_audio_alert_info.shadow_mode, false) as shadow_mode,
    time as alert_ms,
    value.proto_value.cm3x_audio_alert_info.trigger_time_ms,
    value.proto_value.cm3x_audio_alert_info.playback_start_ms,
    value.proto_value.cm3x_audio_alert_info.device_write_start_ms
  from kinesisstats.osdcm3xaudioalertinfo
  where date between '{}' and '{}'
    and value.proto_value.cm3x_audio_alert_info.event_type = 41 -- LOW_BRIDGE_STRIKE_WARNING = 41
""".format(
        start_date, end_date
    )
).fillna(0)

low_bridge_strike_warning_audio_alert_count_df = (
    low_bridge_strike_warning_audio_alerts_df.groupBy(
        "date", "org_id", "device_id", "shadow_mode"
    ).agg(F.count("*").alias("count"))
)

# COMMAND ----------

# Update Low Bridge Strike Warning Audio Alerts Table

create_or_update_table(
    "dataprep_firmware.low_bridge_strike_warning_audio_alerts",
    low_bridge_strike_warning_audio_alerts_df,
    "date",
    ["date", "org_id", "device_id", "alert_ms"],
)

# COMMAND ----------

# Update Low Bridge Strike Warning Audio Alert Count Table

create_or_update_table(
    "dataprep_firmware.low_bridge_strike_warning_audio_alert_count",
    low_bridge_strike_warning_audio_alert_count_df,
    "date",
    ["date", "org_id", "device_id", "shadow_mode"],
)
