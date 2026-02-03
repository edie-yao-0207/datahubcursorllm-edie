# Databricks notebook source

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

# full_days_where_column_is_true returns a df with an entry for each day where col_name is true
# for the full day
# Returns:
#   pyspark dataframe with columns [date, org_id, device_id, `col_name`]
def full_days_where_column_is_true(stat_df, col_name):
    # Convert col_name to value.int_value to match create_intervals expected input
    parsed_int_val_df = stat_df.withColumn(
        "value",
        stat_df["value"].withField(
            "int_value", F.when(stat_df[col_name], 1).otherwise(0)
        ),
    )
    # Days where col is true for some amount of time
    col_true_daily = create_intervals_daily(
        create_intervals(parsed_int_val_df, lambda x: x > 0, query_end_ms),
        start_date,
        end_date,
    )
    # Days where col is true for the full day
    col_true_full_days = col_true_daily.filter(
        F.col("end_ms") - F.col("start_ms") == day_ms() - 1
    ).select("date", "org_id", "device_id")
    return col_true_full_days.withColumn(col_name, F.lit(True))


# decorate_with_days_where_column_is_true adds the column `col_name` to the to_decorate, where the added column is true for
# all days where stat_df[col_name] is true for the full day, and false otherwise
def decorate_with_days_where_column_is_true(to_decorate, stat_df, col_name):
    col_true_full_day = full_days_where_column_is_true(stat_df, col_name)

    # Add bridge location information to enabled devices
    decorated_enabled_df = to_decorate.join(
        col_true_full_day, on=["date", "org_id", "device_id"], how="left"
    )
    # Coalesce to col_name False if not set
    decorated_enabled_df = decorated_enabled_df.withColumn(
        col_name,
        F.coalesce(decorated_enabled_df[col_name], F.lit(False)),
    )
    return decorated_enabled_df


# COMMAND ----------

# Build Adoption Table

tiling_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      first(s3_proto_value.reported_device_config.device_config.tile_manager_config.enabled) as tiling_enabled,
      case
        when first(s3_proto_value.reported_device_config.device_config.tile_manager_config.bridge_location_enabled) = 1 then true
        else false
      end as bridge_location_enabled,
      named_struct(
      "int_value", 0,
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
# Get all days where tiling was enabled
daily_tiling_enabled_cfg_df = full_days_where_column_is_true(
    tiling_enabled_cfg_df, "tiling_enabled"
)
# Decorate daily enabled df with bridge location enabled status
daily_tiling_enabled_cfg_df = decorate_with_days_where_column_is_true(
    daily_tiling_enabled_cfg_df, tiling_enabled_cfg_df, "bridge_location_enabled"
)

# COMMAND ----------

# Update Adoption Table

create_or_update_table(
    "dataprep_firmware.tiling_daily_enabled",
    daily_tiling_enabled_cfg_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Build Active Device Table

daily_tiling_active_device_df = spark.sql(
    """
  with tiling_trip_metadata as (
    select
      a.date,
      a.org_id,
      a.device_id,
      a.bridge_location_enabled,
      max(total_trips) as trip_count,
      max(total_distance_meters) / 1609 as trip_miles
    from dataprep_firmware.tiling_daily_enabled as a
    join dataprep_safety.cm_device_health_daily as b
      on
        a.device_id = b.cm_device_id
        and a.org_id = b.org_id
        and a.date = b.date
    where
      b.date >= date_sub('{}', {})
      and b.date <= '{}'
    group by
      a.date,
      a.org_id,
      a.device_id,
      a.bridge_location_enabled
  )
  select
    *
  from
    tiling_trip_metadata
  where
    trip_count > 0
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

# COMMAND ----------

# Update Active Device Table
create_or_update_table(
    "dataprep_firmware.tiling_daily_active",
    daily_tiling_active_device_df,
    "date",
    ["date", "org_id", "device_id"],
)

# Build Layers Published Table
# The old "did_xyz_lookup" field can be derived by joining this table with tiling_daily_active_with_layer_config
osdcurrentmaptile_layers_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    value.proto_value.current_tile_info.slippy_tile as slippy_tile,
    coalesce(layer.layer_name, "NO-LAYER-NAME") as layer_name,
    coalesce(layer.version, "NO-VERSION") as version,
    coalesce(layer.cache_key, "NO-CACHE-KEY") as cache_key,
    coalesce(layer.file_exists, false) as file_exists,
    coalesce(layer.file_on_disk_updated, false) as file_on_disk_updated,
    concat_ws("-", layer.version, layer.cache_key) as version_and_cache_key
  from kinesisstats.osdcurrentmaptile LATERAL VIEW EXPLODE(value.proto_value.current_tile_info.layers) as layer
  where date >= '{}'
    and date <= '{}'
""".format(
        start_date, end_date
    )
)

daily_tile_layer_published_count_df = osdcurrentmaptile_layers_df.groupBy(
    "date",
    "org_id",
    "device_id",
    "slippy_tile",
    "layer_name",
    "version",
    "cache_key",
    "file_exists",
    "file_on_disk_updated",
    "version_and_cache_key",
).agg(F.count("*").alias("count"))

# COMMAND ----------

# Update Published Layers Table
create_or_update_table(
    "dataprep_firmware.tiling_published_layer_count",
    daily_tile_layer_published_count_df,
    "date",
    [
        "date",
        "org_id",
        "device_id",
        "slippy_tile",
        "layer_name",
        "version",
        "cache_key",
        "file_exists",
        "file_on_disk_updated",
        "version_and_cache_key",
    ],
)

# COMMAND ----------

# Build Anomalies Table

anomalies_df = spark.sql(
    """
  select
    date,
    org_id,
    object_id as device_id,
    value.proto_value.anomaly_event.description as description
  from kinesisstats.osdanomalyevent
  where date >= '{}'
    and date <= '{}'
    and value.proto_value.anomaly_event.service_name like '%speedlimit%'
""".format(
        start_date, end_date
    )
)
daily_anomaly_count_df = (
    anomalies_df.groupBy("date", "org_id", "device_id", "description").agg(
        F.count("*").alias("count"),
    )
).na.fill(0)


# COMMAND ----------

# Update Anomamlies Table

create_or_update_table(
    "dataprep_firmware.tiling_anomalies",
    daily_anomaly_count_df,
    "date",
    ["date", "org_id", "device_id", "description"],
)
