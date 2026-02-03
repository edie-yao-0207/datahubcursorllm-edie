# COMMAND ----------

# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# Query this many days before the start_date to get the previous value
# for state based object stats which don't change often
prev_value_lookback_days = 7

query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

# Parse device configs into days with speeding enabled


def days_enabled_for_full_day_with_audio_enabled(stat_df):
    # Days where speeding was enabled for some amount of time
    speeding_enabled_parsed = stat_df.withColumn(
        "value",
        stat_df["value"].withField(
            "int_value", F.when(stat_df["speeding_enabled"], 1).otherwise(0)
        ),
    )
    speeding_enabled_daily = create_intervals_daily(
        create_intervals(speeding_enabled_parsed, lambda x: x > 0, query_end_ms),
        start_date,
        end_date,
    )
    # Days where speeding was enabled for the full day
    speeding_enabled_full_days = speeding_enabled_daily.filter(
        F.col("end_ms") - F.col("start_ms") == day_ms() - 1
    ).select("date", "org_id", "device_id")

    # Days where speeding audio alerts were enabled for some amount of time
    audio_alert_enabled_parsed = stat_df.withColumn(
        "value",
        stat_df["value"].withField(
            "int_value", F.when(stat_df["audio_alerts_enabled"], 1).otherwise(0)
        ),
    )
    audio_alerts_enabled_daily = create_intervals_daily(
        create_intervals(audio_alert_enabled_parsed, lambda x: x > 0, query_end_ms),
        start_date,
        end_date,
    )
    # Days where speeding audio alerts were enabled for the full day
    audio_alerts_enabled_full_days = audio_alerts_enabled_daily.filter(
        F.col("end_ms") - F.col("start_ms") == day_ms() - 1
    ).select("date", "org_id", "device_id")
    audio_alerts_enabled_full_days = audio_alerts_enabled_full_days.withColumn(
        "audio_alerts_enabled", F.lit(True)
    )

    # Add audio alert information to enabled devices
    speeding_enabled_full_days = speeding_enabled_full_days.join(
        audio_alerts_enabled_full_days, on=["date", "org_id", "device_id"], how="left"
    )
    speeding_enabled_full_days = speeding_enabled_full_days.withColumn(
        "audio_alerts_enabled",
        F.coalesce(speeding_enabled_full_days["audio_alerts_enabled"], F.lit(False)),
    )
    return speeding_enabled_full_days.select(
        "date", "org_id", "device_id", "audio_alerts_enabled"
    )


# COMMAND ----------

# Devices with speeding enabled

daily_speeding_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      s3_proto_value.reported_device_config.device_config.speed_limit_alert.enabled as speeding_enabled,
      s3_proto_value.reported_device_config.device_config.speed_limit_alert.enable_audio_alerts as audio_alerts_enabled,
      named_struct(
        "int_value", 0,
        "proto_value", null,
        "is_databreak", false
      ) as value
  from s3bigstats.osdreporteddeviceconfig_raw
  where date >= date_sub('{}', {})
    and date <= '{}'
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

daily_speeding_enabled_full_day_df = days_enabled_for_full_day_with_audio_enabled(
    daily_speeding_enabled_cfg_df
)

# COMMAND ----------

# Update Adoption Table

create_or_update_table(
    "dataprep_firmware.speeding_daily_devices",
    daily_speeding_enabled_full_day_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Build Active Device Table

daily_speeding_active_device_df = spark.sql(
    """
  select
    a.date,
    a.org_id,
    a.device_id,
    a.audio_alerts_enabled,
    max(t.trip_count) as trip_count,
    max(t.total_distance_meters) / 1609 as trip_miles
  from dataprep_firmware.speeding_daily_devices as a
    join dataprep_firmware.cm_octo_vg_daily_associations_unique as assoc
      on a.device_id = assoc.cm_device_id
      and a.date = assoc.date
    join datamodel_telematics.fct_trips_daily as t
      on assoc.vg_device_id = t.device_id
      and a.org_id = t.org_id
      and a.date = t.date
      and t.trip_type = 'location_based'
  where
    a.date between '{}' and '{}'
    and t.trip_count > 0
  group by
    a.date,
    a.org_id,
    a.device_id,
    a.audio_alerts_enabled
  """.format(
        start_date, end_date
    )
)

# COMMAND ----------

# Update Active Device Table

create_or_update_table(
    "dataprep_firmware.speeding_daily_active_devices",
    daily_speeding_active_device_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Build Uptime Table


# Build Map Matching And Has Speed Limit Intervals

osdedgespeedlimitdf = spark.sql(
    """
  select
    date,
    time,
    org_id,
    object_id,
    named_struct("int_value",
      case
        when value.proto_value.edge_speed_limit.has_edge_speed_limit is true then 2
        when value.proto_value.edge_speed_limit is not null and value.proto_value.edge_speed_limit.has_edge_speed_limit is null then 1
        else 0
      end,
      "proto_value", null,
      "is_databreak", false) as value
  from kinesisstats.osdedgespeedlimitv2
  where date >= date_sub('{}', {})
    and date <= '{}'
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

map_matching_intervals_df = create_intervals(
    osdedgespeedlimitdf, lambda x: x > 0, query_end_ms
)
has_speed_limit_intervals_df = create_intervals(
    osdedgespeedlimitdf, lambda x: x == 2, query_end_ms
)


# Build Trip Interval Table

active_devices_df = spark.sql(
    "select * from dataprep_firmware.speeding_daily_active_devices"
)

trip_intervals_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id,
    proto.start.time as start_ms,
    proto.end.time as end_ms
  from trips2db_shards.trips
  where date >= date_sub('{}', {})
    and date <= '{}'
    and version = 101
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

vg_cm_assoc_df = spark.sql(
    """
  select
    assoc.date,
    assoc.cm_device_id,
    assoc.vg_device_id,
    gw.org_id
  from dataprep_firmware.cm_octo_vg_daily_associations_unique as assoc
  join datamodel_core_bronze.raw_productsdb_gateways as gw
    on assoc.vg_gateway_id = gw.id
    and assoc.date = gw.date
  where assoc.date >= date_sub('{}', {})
    and assoc.date <= '{}'
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

relevant_trip_intervals_df = (
    trip_intervals_df.alias("a")
    .join(
        vg_cm_assoc_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.vg_device_id"))
        & (F.col("a.date") == F.col("b.date")),
    )
    .join(
        active_devices_df.alias("c"),
        (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("b.cm_device_id") == F.col("c.device_id"))
        & (F.col("a.date") == F.col("c.date")),
    )
    .select(
        "a.date",
        "a.org_id",
        F.col("b.cm_device_id").alias("device_id"),
        "a.start_ms",
        "a.end_ms",
    )
)


# Intersect Uptime with Trip Interval

daily_map_matching_while_on_trip_intervals_df = create_intervals_daily(
    intersect_intervals(relevant_trip_intervals_df, map_matching_intervals_df),
    start_date,
    end_date,
)
daily_map_matching_time_df = daily_map_matching_while_on_trip_intervals_df.groupBy(
    "date", "org_id", "device_id"
).agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("map_matching_duration_ms"))

daily_has_speed_limit_while_on_trip_intervals_df = create_intervals_daily(
    intersect_intervals(relevant_trip_intervals_df, has_speed_limit_intervals_df),
    start_date,
    end_date,
)
daily_has_speed_limit_time_df = (
    daily_has_speed_limit_while_on_trip_intervals_df.groupBy(
        "date", "org_id", "device_id"
    ).agg(
        F.sum(F.col("end_ms") - F.col("start_ms")).alias("has_speed_limit_duration_ms")
    )
)

daily_active_device_trip_time_df = spark.sql(
    """
  select
    a.date,
    a.org_id,
    a.device_id,
    max(t.total_duration_mins * 60000) as total_trip_duration_ms,
    max(t.trip_count) as total_trips
  from dataprep_firmware.speeding_daily_active_devices a
    join dataprep_firmware.cm_octo_vg_daily_associations_unique as assoc
      on a.device_id = assoc.cm_device_id
      and a.date = assoc.date
    join datamodel_telematics.fct_trips_daily t
      on assoc.vg_device_id = t.device_id
      and a.org_id = t.org_id
      and a.date = t.date
      and t.trip_type = 'location_based'
  where a.date between '{}' and '{}'
  group by
    a.date,
    a.org_id,
    a.device_id
""".format(
        start_date, end_date
    )
)

daily_map_matching_speedlimit_uptime_df = (
    daily_active_device_trip_time_df.alias("a")
    .join(
        daily_map_matching_time_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id")),
        how="left",
    )
    .join(
        daily_has_speed_limit_time_df.alias("c"),
        (F.col("a.date") == F.col("c.date"))
        & (F.col("a.org_id") == F.col("c.org_id"))
        & (F.col("a.device_id") == F.col("c.device_id")),
        how="left",
    )
    .select(
        "a.date",
        "a.org_id",
        "a.device_id",
        "a.total_trip_duration_ms",
        "a.total_trips",
        F.coalesce(F.col("b.map_matching_duration_ms"), F.lit(0)).alias(
            "map_matching_duration_ms"
        ),
        F.coalesce(F.col("c.has_speed_limit_duration_ms"), F.lit(0)).alias(
            "has_speed_limit_duration_ms"
        ),
    )
)

# COMMAND ----------

# Update Uptime Table

create_or_update_table(
    "dataprep_firmware.speeding_map_matching_speedlimit_uptime",
    daily_map_matching_speedlimit_uptime_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Build Map Matching Errors Table

map_matching_errors_df = spark.sql(
    """
  select
  date,
  org_id,
  object_id as device_id,
  case
    when value.proto_value.edge_speed_limit.error_message like "%error: no gps data processed%" then "error: no gps processed for xxx ms"
    when value.proto_value.edge_speed_limit.error_message like "%is outside of tile bounding box%" then "latest point is outside of tile bounding box"
    when value.proto_value.edge_speed_limit.error_message like "%dial tcp 127.0.0.1:5000: connect: connection refused%" then "Get http://127.0.0.1:5000/match/v1/driving/...: dial tcp 127.0.0.1:5000: connect: connection refused"
    when value.proto_value.edge_speed_limit.error_message like "%read: connection reset by peer%" then "Get http://127.0.0.1:5000/match/v1/driving/...: read tcp 127.0.0.1:xxx->127.0.0.1:5000 read: connection reset by peer"
    when value.proto_value.edge_speed_limit.error_message like "%dial tcp 127.0.0.1:5000: connect: connection reset by peer%" then "Get http://127.0.0.1:5000/match/v1/driving/...: dial tcp 127.0.0.1:5000: connect: connection reset by peer"
    when value.proto_value.edge_speed_limit.error_message like "%write: connection reset by peer%" then "Get http://127.0.0.1:5000/match/v1/driving/...: write tcp 127.0.0.1:xxx->127.0.0.1:5000: write: connection reset by peer"
    when value.proto_value.edge_speed_limit.error_message like "%write: broken pipe%" then "Get http://127.0.0.1:5000/match/v1/driving/...: write tcp 127.0.0.1:xxx->127.0.0.1:5000: write: broken pipe"
    when value.proto_value.edge_speed_limit.error_message like "%context deadline exceeded%" then "Get http://127.0.0.1:5000/match/v1/driving/...: context deadline exceeded"
    when value.proto_value.edge_speed_limit.error_message like "%EOF%" then "Get http://127.0.0.1:5000/match/v1/driving/...: EOF"
    when value.proto_value.edge_speed_limit.error_message like "%dial tcp 127.0.0.1:5000: i/o timeout%" then "Get http://127.0.0.1:5000/match/v1/driving/...: dial tcp 127.0.0.1:5000: i/o timeout"
    when value.proto_value.edge_speed_limit.error_message like "%http: server closed idle connection%" then "Get http://127.0.0.1:5000/match/v1/driving/...: http: server closed idle connection"
    else value.proto_value.edge_speed_limit.error_message
  end as error_message
from kinesisstats.osdedgespeedlimitv2
where date between '{}' and '{}'
  and value.proto_value.edge_speed_limit.error_message != ""
""".format(
        start_date, end_date
    )
)

map_matching_errors_count_df = map_matching_errors_df.groupBy(
    "date", "org_id", "device_id", "error_message"
).agg(F.count("*").alias("count"))

# COMMAND ----------

# Update Map Matching Errors Table

create_or_update_table(
    "dataprep_firmware.speeding_map_matching_errors_count",
    map_matching_errors_count_df,
    "date",
    ["date", "org_id", "device_id", "error_message"],
)

# COMMAND ----------

# Build Speed Limit Count Table

# EdgeSpeedLimits, exploded on layer info. To get version and cache key of the layer source, we filter out rows where speed_limit_layer_source != layer_name
osdedgespeedlimitdf = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    value.proto_value.edge_speed_limit.speed_limit_source,
    value.proto_value.edge_speed_limit.speed_limit_vehicle_type,
    value.proto_value.edge_speed_limit.speed_limit_layer_source,
    coalesce(layer.version, "NO-VERSION") as layer_version,
    coalesce(layer.cache_key, "NO-CACHE-KEY") as layer_cache_key,
    concat_ws("-", layer.version, layer.cache_key) as layer_version_cache_key
  from kinesisstats.osdedgespeedlimitv2 lateral view explode(value.proto_value.edge_speed_limit.current_tile_info.layers) as layer
  where date between '{}' and '{}'
    and value.proto_value.edge_speed_limit.has_edge_speed_limit is true
    and layer.layer_name = value.proto_value.edge_speed_limit.speed_limit_layer_source
""".format(
        start_date, end_date
    )
)

# Get daily count
speed_limit_count_df = (
    osdedgespeedlimitdf.groupBy(
        "date",
        "org_id",
        "device_id",
        "speed_limit_source",
        "speed_limit_vehicle_type",
        "speed_limit_layer_source",
        "layer_version",
        "layer_cache_key",
        "layer_version_cache_key",
    )
    .agg(F.count("*").alias("count"))
    .fillna(0)  # Replace null values with 0 to allow tables to update properly
)

# COMMAND ----------

# Update Speed Limit Count Table

create_or_update_table(
    "dataprep_firmware.speeding_speed_limit_count",
    speed_limit_count_df,
    "date",
    [
        "date",
        "org_id",
        "device_id",
        "speed_limit_source",
        "speed_limit_vehicle_type",
        "speed_limit_layer_source",
        "layer_version",
        "layer_cache_key",
        "layer_version_cache_key",
    ],
)

# COMMAND ----------

# Build Speeding Audio Alert Count Table

speeding_audio_alert_count_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    count(*) as count
  from kinesisstats.osdcm3xaudioalertinfo
  where date between '{}' and '{}'
    and value.proto_value.cm3x_audio_alert_info.event_type = 5 -- SPEEDING_ALERT_EVENT = 5
  group by
    date,
    org_id,
    object_id
""".format(
        start_date, end_date
    )
)

# COMMAND ----------

# Update Speeding Audio Alert Count Table

create_or_update_table(
    "dataprep_firmware.speeding_audio_alert_count",
    speeding_audio_alert_count_df,
    "date",
    ["date", "org_id", "device_id"],
)
