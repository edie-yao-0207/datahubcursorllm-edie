# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 0)
prev_value_lookback_days = 14

# COMMAND ----------

query_start_ms = to_ms(start_date)
query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

osdparkingmodestate_df = spark.sql(
    """
  select a.*
  from kinesisstats.osdparkingmodestate as a
  join dataprep_firmware.data_ingestion_high_water_mark as hwm
  where a.date >= date_sub('{}', {})
    and a.date <= '{}'
    and a.time <= hwm.time_ms
""".format(
        start_date, prev_value_lookback_days, end_date
    )
)

# Add proto_value field with null value if it doesn't exist to use helper function create_intervals
osdparkingmodestate_df = osdparkingmodestate_df.withColumn(
    "value", F.struct(F.col("value.*"), F.lit(None).cast("string").alias("proto_value"))
)

parking_mode_intervals_df = create_intervals_with_state_value(
    osdparkingmodestate_df,
    "value.int_value",
    query_start_ms,
    query_end_ms,
).withColumnRenamed("state_value", "parking_mode_state")

# COMMAND ----------

dashcam_target_state_intervals_df = spark.sql(
    """
    select *
    from dataprep_firmware.cm_dashcam_target_state_intervals
    where date >= date_sub('{}', {})
      and date <= '{}'
    """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

dashcam_target_state_intervals_df = dashcam_target_state_intervals_df.withColumn(
    "target_parking_mode_state",
    F.when(
        (F.size(F.col("recording_reason")) == 1) & (F.col("recording_reason")[0] == 2),
        1,
    ).otherwise(0),
)

# COMMAND ----------

dashcam_actual_and_target_parking_mode_intervals_daily_df = (
    (
        intersect_intervals(
            dashcam_target_state_intervals_df.filter(
                F.col("target_parking_mode_state") == 1
            ),
            parking_mode_intervals_df.filter(F.col("parking_mode_state") == 1),
        )
        .withColumn("is_target_parking_mode", F.lit(1))
        .withColumn("is_actual_parking_mode", F.lit(1))
    )
    .union(
        intersect_intervals(
            dashcam_target_state_intervals_df.filter(
                F.col("target_parking_mode_state") != 1
            ),
            parking_mode_intervals_df.filter(F.col("parking_mode_state") == 1),
        )
        .withColumn("is_target_parking_mode", F.lit(0))
        .withColumn("is_actual_parking_mode", F.lit(1))
    )
    .union(
        intersect_intervals(
            dashcam_target_state_intervals_df.filter(
                F.col("target_parking_mode_state") == 1
            ),
            parking_mode_intervals_df.filter(F.col("parking_mode_state") != 1),
        )
        .withColumn("is_target_parking_mode", F.lit(1))
        .withColumn("is_actual_parking_mode", F.lit(0))
    )
    .union(
        intersect_intervals(
            dashcam_target_state_intervals_df.filter(
                F.col("target_parking_mode_state") != 1
            ),
            parking_mode_intervals_df.filter(F.col("parking_mode_state") != 1),
        )
        .withColumn("is_target_parking_mode", F.lit(0))
        .withColumn("is_actual_parking_mode", F.lit(0))
    )
)

# COMMAND ----------

# Exclude intervals of a device in a day if all of its intervals within that
# day have both target and actual parking mode states zero.
window_spec = Window.partitionBy("date", "org_id", "device_id")
dashcam_actual_and_target_parking_mode_intervals_daily_df = (
    dashcam_actual_and_target_parking_mode_intervals_daily_df.withColumn(
        "daily_sum_is_target_parking_mode",
        F.sum("is_target_parking_mode").over(window_spec),
    )
    .withColumn(
        "daily_sum_is_actual_parking_mode",
        F.sum("is_actual_parking_mode").over(window_spec),
    )
    .filter(
        (F.col("daily_sum_is_target_parking_mode") > 0)
        | (F.col("daily_sum_is_actual_parking_mode") > 0)
    )
    .drop("daily_sum_is_target_parking_mode", "daily_sum_is_actual_parking_mode")
)

# COMMAND ----------

create_or_update_table(
    "dataprep_firmware.cm_actual_and_target_parking_mode_intervals_daily",
    dashcam_actual_and_target_parking_mode_intervals_daily_df.dropDuplicates(
        ["date", "org_id", "device_id", "start_ms"]
    ),
    "date",
    ["date", "org_id", "device_id", "start_ms"],
)
