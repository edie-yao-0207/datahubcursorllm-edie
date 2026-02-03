# COMMAND ----------

# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# Query this many days before the start_date to get the previous value
# for state based object stats which don't change often
prev_value_lookback_days = 7

query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

# days_enabled_for_full_day_with_enabled_type gets days where one version of maxspeed was enabled for the full day
# and tags the day with the "enabled_type" column, which is filled with which version was enabled on that day
# enabled_type = (Legacy | MaxSpeedManager | Both)
def days_enabled_for_full_day_with_enabled_type(stat_df):
    stat_intervals_daily_df = create_intervals_daily(
        create_intervals(stat_df, lambda x: x > 0, query_end_ms), start_date, end_date
    )

    # Get days where legacy was enabled for some time
    legacy_enabled = create_intervals_daily(
        create_intervals(stat_df, lambda x: x == 1, query_end_ms), start_date, end_date
    ).select("date", "org_id", "device_id")
    # Ensure only 1 row per day
    legacy_enabled = legacy_enabled.dropDuplicates()
    # Mark as legacy
    legacy_enabled = legacy_enabled.withColumn("is_legacy_enabled", F.lit(True))

    # Get days where legacy was enabled for some time
    msm_enabled = create_intervals_daily(
        create_intervals(stat_df, lambda x: x == 2, query_end_ms), start_date, end_date
    ).select("date", "org_id", "device_id")
    # Ensure only 1 row per day
    msm_enabled = msm_enabled.dropDuplicates()
    # Mark as maxspeedmanager
    msm_enabled = msm_enabled.withColumn("is_maxspeedmanager_enabled", F.lit(True))

    stat_full_days_df = stat_intervals_daily_df.filter(
        F.col("end_ms") - F.col("start_ms") == day_ms() - 1
    ).select("date", "org_id", "device_id")

    # Join with days where legacy system was enabled for some amount of time
    stat_full_days_df = stat_full_days_df.join(
        legacy_enabled, on=["date", "org_id", "device_id"], how="left"
    )
    # Join with days where MaxSpeedMananger was enabled for some amount of time
    stat_full_days_df = stat_full_days_df.join(
        msm_enabled, on=["date", "org_id", "device_id"], how="left"
    )

    return stat_full_days_df.select(
        "date", "org_id", "device_id", "is_legacy_enabled", "is_maxspeedmanager_enabled"
    )


# COMMAND ----------

# Build Adoption Table

daily_maxspeed_enabled_cfg_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      first(s3_proto_value.reported_device_config.device_config.obd_config.obd_max_speed_alert_enabled) = 1 as is_legacy_enabled,
      first(s3_proto_value.reported_device_config.device_config.max_speed_alert.enabled) = 1 as is_maxspeedmanager_enabled,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.obd_config.obd_max_speed_alert_enabled) = 1 then 1
        when first(s3_proto_value.reported_device_config.device_config.max_speed_alert.enabled) = 1 then 2
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

daily_maxspeed_enabled_full_day_df = days_enabled_for_full_day_with_enabled_type(
    daily_maxspeed_enabled_cfg_df
)

# Get CM pairings
# VGs can have more than 1 CM connected per day. This leads to a VG having multiple rows with different
# cm_device_id columns. We select last(cm_device_id) to ensure we don't count the same VG more than once.
daily_cm_pairing_df = spark.sql(
    """
  select
    date,
    org_id,
    device_id,
    last(cm_device_id) as cm_device_id
  from
    dataprep_safety.cm_device_health_daily
  group by
    date,
    org_id,
    device_id
""".format(
        start_date, end_date
    )
)

daily_maxspeed_enabled_full_day_df = daily_maxspeed_enabled_full_day_df.join(
    daily_cm_pairing_df, on=["date", "org_id", "device_id"], how="left"
)

# COMMAND ----------

# Update Adoption Table

create_or_update_table(
    "dataprep_firmware.maxspeed_daily_enabled",
    daily_maxspeed_enabled_full_day_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Build Active Device Table

daily_maxspeed_active_device_df = spark.sql(
    """
  select
    a.date,
    a.org_id,
    a.device_id,
    a.cm_device_id,
    max(b.trip_count) as trip_count,
    max(b.total_distance_meters) / 1609 as trip_miles
  from dataprep_firmware.maxspeed_daily_enabled as a
  join data_analytics.vg3x_daily_summary as b
    on a.device_id = b.device_id
    and a.org_id = b.org_id
    and a.date = b.date
  where
    a.date between '{}' and '{}'
    and trip_count > 0
  group by
    a.date,
    a.org_id,
    a.device_id,
    a.cm_device_id
  """.format(
        start_date, end_date
    )
)

# COMMAND ----------

# Update Active Device Table

create_or_update_table(
    "dataprep_firmware.maxspeed_daily_active",
    daily_maxspeed_active_device_df,
    "date",
    ["date", "org_id", "device_id"],
)

# COMMAND ----------

# Build Audio Alert Count Table

maxspeed_audio_alert_count_df = spark.sql(
    """
  select
    a.date,
    a.org_id,
    b.vg_device_id,
    count(*) as count
  from kinesisstats.osdcm3xaudioalertinfo as a
  join dataprep_safety.cm_device_daily_metadata as b
    on a.date = b.date
    and a.org_id = b.org_id
    and a.object_id = b.cm_device_id
  where a.date between '{}' and '{}'
    and value.proto_value.cm3x_audio_alert_info.event_type = 21 -- VG_GENERATED_OBD_SPEED_EVENT = 21
  group by
    a.date,
    a.org_id,
    b.vg_device_id
""".format(
        start_date, end_date
    )
)

# COMMAND ----------

# Update Audio Alert Count Table

create_or_update_table(
    "dataprep_firmware.maxspeed_audio_alert_count",
    maxspeed_audio_alert_count_df,
    "date",
    ["date", "org_id", "vg_device_id"],
)
