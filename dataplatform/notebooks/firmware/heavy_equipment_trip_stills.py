# Databricks notebook source

# COMMAND ----------

# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# Query this many days before the start_date to get the previous value
# for state based object stats which don't change often
prev_value_lookback_days = 7

# COMMAND ----------

query_start_ms = to_ms(start_date)
query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

# Acceptable time difference (plus or minus) between expected still capture and actual still capture
still_time_error_ms = 10 * 1000
periodic_still_period_ms = 6 * 60 * 1000

# COMMAND ----------

equipment_activity_df = spark.sql(
    """
  select
    date,
    time,
    org_id,
    object_id as device_id,
    named_struct(
    "int_value", value.proto_value.equipment_activity.state,
    "proto_value", null,
    "is_databreak", value.is_databreak) as value
  from kinesisstats_history.osdequipmentactivity
  where date >= date_sub('{}', {})
      and date <= '{}'
  """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

stills_df = spark.sql(
    """
  select
    date,
    time,
    org_id,
    object_id as device_id,
    value.proto_value.dashcam_report.trigger_reason
  from kinesisstats_history.osddashcamreport
  where date >= '{}'
      and date <= '{}'
      and value.proto_value.dashcam_report.report_type = 1
  """.format(
        start_date, end_date
    )
)

vgs_with_active_cms_df = spark.sql(
    """
  select
    a.date,
    a.org_id,
    b.vg_device_id as device_id
  from dataprep_firmware.active_devices_based_on_heartbeat_in_last_14_days as a
  join dataprep_firmware.cm_octo_vg_daily_associations_unique as b
    on a.date = b.date
    and a.device_id = b.cm_device_id
  where a.date >= '{}'
      and a.date <= '{}'
  """.format(
        start_date, end_date
    )
)

heavy_equipment_stills_enabled_df = spark.sql(
    """
   select
      date,
      time,
      org_id,
      object_id,
      named_struct("int_value",
      case
        when first(s3_proto_value.reported_device_config.device_config.still_manager.operating_mode) = 2 then 1
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

# COMMAND ----------


# Change the last known state from true to false if we haven't seen gotten a new stat in the last 15 min
# since we expect a stat update every 10 min.
lead_equipment_activity_df = equipment_activity_df.withColumn(
    "next_time",
    F.lead(F.col("time"), 1).over(
        Window.partitionBy("org_id", "device_id").orderBy("time")
    ),
)
equipment_activity_with_timeout_df = lead_equipment_activity_df.select(
    "date",
    "time",
    "org_id",
    "device_id",
    F.struct(
        # The current interval is valid only if the next datapoint is within
        # 15 min because the stat is reported every 10 min.
        F.when(
            (F.col("value.int_value") == 1)
            & (F.col("next_time").isNotNull())
            & (F.col("next_time") - F.col("time") < 15 * 60 * 1000),
            1,
        )
        .otherwise(0)
        .alias("int_value"),
        F.col("value.proto_value"),
        F.col("value.is_databreak"),
    ).alias("value"),
)

# Create the intervals of equipment activity. Add a date based on the start of interval timestamp.
equipment_activity_intervals_df = create_intervals_v2(
    equipment_activity_with_timeout_df, lambda x: x == 1, query_start_ms, query_end_ms
).withColumn("date", F.from_unixtime(F.col("start_ms") / 1000))

# Get the days when heavy equipment trip stills are enabled for the entire day.
heavy_equipment_stills_enabled_full_days_df = create_intervals_daily(
    create_intervals_v2(
        heavy_equipment_stills_enabled_df,
        lambda x: x == 1,
        query_start_ms,
        query_end_ms,
    ),
    start_date,
    end_date,
).filter("end_ms - start_ms = 86400000-1")

# Get the heavy equipment intervals (based on the date of the interval start)
#  where the CM is active and trip stills are enabled for heavy
# equipment.
equipment_activity_intervals_filtered_df = (
    equipment_activity_intervals_df.join(
        vgs_with_active_cms_df, ["date", "org_id", "device_id"]
    )
    .join(heavy_equipment_stills_enabled_full_days_df, ["date", "org_id", "device_id"])
    .select(equipment_activity_intervals_df["*"])
    .filter(F.col("end_ms") != query_end_ms)
)

trip_start_stills_df = stills_df.filter(F.col("trigger_reason") == 1)
trip_end_stills_df = stills_df.filter(
    (F.col("trigger_reason").isNull()) | (F.col("trigger_reason") == 0)
)
trip_periodic_stills_df = stills_df.filter(F.col("trigger_reason") == 3)

# Join the heavy equipment intervals on each type of trip stills and count the number of stills at the start of the trip, the end of the trip and
# during the trip.
equipment_activity_intervals_with_trip_start_stills_df = (
    equipment_activity_intervals_filtered_df.alias("a")
    .join(
        trip_start_stills_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (
            F.col("a.start_ms").between(
                F.col("b.time") - F.lit(still_time_error_ms),
                F.col("b.time") + F.lit(still_time_error_ms),
            )
        ),
        "left",
    )
    .groupBy(
        "a.org_id",
        "a.device_id",
        "a.start_ms",
        "a.end_ms",
    )
    .agg(
        F.sum(F.when(F.col("b.time").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias(
            "actual_num_trip_start_stills"
        )
    )
    .select(
        "a.org_id",
        "a.device_id",
        "a.start_ms",
        "a.end_ms",
        "actual_num_trip_start_stills",
    )
    .alias("a")
    .join(
        trip_end_stills_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (
            F.col("b.time").between(
                F.col("a.end_ms") - F.lit(still_time_error_ms),
                F.col("a.end_ms") + F.lit(still_time_error_ms),
            )
        ),
        "left",
    )
    .groupBy(
        "a.org_id",
        "a.device_id",
        "a.start_ms",
        "a.end_ms",
        "actual_num_trip_start_stills",
    )
    .agg(
        F.sum(F.when(F.col("b.time").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias(
            "actual_num_trip_end_stills"
        )
    )
    .select(
        "a.org_id",
        "a.device_id",
        "a.start_ms",
        "a.end_ms",
        "actual_num_trip_start_stills",
        "actual_num_trip_end_stills",
    )
    .alias("a")
    .join(
        trip_periodic_stills_df.alias("b"),
        (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.device_id"))
        & (F.col("b.time").between(F.col("a.start_ms"), F.col("a.end_ms"))),
        "left",
    )
    .groupBy(
        "a.org_id",
        "a.device_id",
        "a.start_ms",
        "a.end_ms",
        "actual_num_trip_start_stills",
        "actual_num_trip_end_stills",
    )
    .agg(
        F.sum(F.when(F.col("b.time").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias(
            "actual_num_trip_periodic_stills"
        )
    )
    .select(
        "org_id",
        F.col("device_id").alias("vg_device_id"),
        "start_ms",
        "end_ms",
        F.from_unixtime(F.col("start_ms") / 1000).alias("date"),
        F.lit(1).alias("expected_num_trip_start_stills"),
        "actual_num_trip_start_stills",
        F.lit(1).alias("expected_num_trip_end_stills"),
        "actual_num_trip_end_stills",
        F.lit(
            F.floor(
                (F.col("end_ms") - F.col("start_ms")) / F.lit(periodic_still_period_ms)
            )
        ).alias("expected_num_trip_periodic_stills"),
        "actual_num_trip_periodic_stills",
    )
)

final_agg_df = equipment_activity_intervals_with_trip_start_stills_df.groupBy(
    "date", "org_id", "vg_device_id"
).agg(
    F.sum(F.col("expected_num_trip_start_stills")).alias(
        "expected_num_trip_start_stills"
    ),
    F.sum(
        F.when(
            F.col("expected_num_trip_start_stills")
            > F.col("actual_num_trip_start_stills"),
            F.col("expected_num_trip_start_stills")
            - F.col("actual_num_trip_start_stills"),
        ).otherwise(0)
    ).alias("num_missed_trip_start_stills"),
    F.sum(
        F.when(
            F.col("expected_num_trip_start_stills")
            < F.col("actual_num_trip_start_stills"),
            F.col("actual_num_trip_start_stills")
            - F.col("expected_num_trip_start_stills"),
        ).otherwise(0)
    ).alias("num_extra_trip_start_stills"),
    F.sum(F.col("expected_num_trip_end_stills")).alias("expected_num_trip_end_stills"),
    F.sum(
        F.when(
            F.col("expected_num_trip_end_stills") > F.col("actual_num_trip_end_stills"),
            F.col("expected_num_trip_end_stills") - F.col("actual_num_trip_end_stills"),
        ).otherwise(0)
    ).alias("num_missed_trip_end_stills"),
    F.sum(
        F.when(
            F.col("expected_num_trip_end_stills") < F.col("actual_num_trip_end_stills"),
            F.col("actual_num_trip_end_stills") - F.col("expected_num_trip_end_stills"),
        ).otherwise(0)
    ).alias("num_extra_trip_end_stills"),
    F.sum(F.col("expected_num_trip_periodic_stills")).alias(
        "expected_num_trip_periodic_stills"
    ),
    F.sum(
        F.when(
            F.col("expected_num_trip_periodic_stills")
            > F.col("actual_num_trip_periodic_stills"),
            F.col("expected_num_trip_periodic_stills")
            - F.col("actual_num_trip_periodic_stills"),
        ).otherwise(0)
    ).alias("num_missed_trip_periodic_stills"),
    F.sum(
        F.when(
            F.col("expected_num_trip_periodic_stills")
            < F.col("actual_num_trip_periodic_stills"),
            F.col("actual_num_trip_periodic_stills")
            - F.col("expected_num_trip_periodic_stills"),
        ).otherwise(0)
    ).alias("num_extra_trip_periodic_stills"),
)

# COMMAND ----------

create_or_update_table(
    "dataprep_firmware.device_daily_heavy_equipment_trip_stills",
    final_agg_df,
    "date",
    ["date", "org_id", "vg_device_id"],
)
