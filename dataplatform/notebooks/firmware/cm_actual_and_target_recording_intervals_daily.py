# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 0)

# COMMAND ----------

table_name = "dataprep_firmware.cm_actual_and_target_recording_intervals_daily"

# COMMAND ----------

dashcam_state_intervals_df = spark.sql(
    """
    select *
    from dataprep_firmware.cm_dashcam_state_intervals
    where date >= '{}'
        and date <= '{}'
    """.format(
        start_date, end_date
    )
)

# COMMAND ----------

dashcam_target_state_intervals_df = spark.sql(
    """
    select *
    from dataprep_firmware.cm_dashcam_target_state_intervals
    where date >= '{}'
        and date <= '{}'
    """.format(
        start_date, end_date
    )
)

# COMMAND ----------

dashcam_actual_and_target_recording_intervals_daily_df = (
    (
        intersect_intervals(
            dashcam_target_state_intervals_df.filter(
                F.size(F.col("recording_reason")) > 0
            ),
            dashcam_state_intervals_df.filter(F.size(F.col("recording_reason")) > 0),
        )
        .withColumn("is_target_recording", F.lit(1))
        .withColumn("is_actual_recording", F.lit(1))
    )
    .union(
        intersect_intervals(
            dashcam_target_state_intervals_df.filter(
                F.size(F.col("recording_reason")) == 0
            ),
            dashcam_state_intervals_df.filter(F.size(F.col("recording_reason")) > 0),
        )
        .withColumn("is_target_recording", F.lit(0))
        .withColumn("is_actual_recording", F.lit(1))
    )
    .union(
        intersect_intervals(
            dashcam_target_state_intervals_df.filter(
                F.size(F.col("recording_reason")) > 0
            ),
            dashcam_state_intervals_df.filter(F.size(F.col("recording_reason")) == 0),
        )
        .withColumn("is_target_recording", F.lit(1))
        .withColumn("is_actual_recording", F.lit(0))
    )
    .union(
        intersect_intervals(
            dashcam_target_state_intervals_df.filter(
                F.size(F.col("recording_reason")) == 0
            ),
            dashcam_state_intervals_df.filter(F.size(F.col("recording_reason")) == 0),
        )
        .withColumn("is_target_recording", F.lit(0))
        .withColumn("is_actual_recording", F.lit(0))
    )
)

# COMMAND ----------

# Capture a single consistent timestamp for the entire job,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across partitions.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

# Exclude intervals of a device in a day if all of its intervals within that
# day have both target and actual recording states zero.
window_spec = Window.partitionBy("date", "org_id", "device_id")
dashcam_actual_and_target_recording_intervals_daily_df = (
    dashcam_actual_and_target_recording_intervals_daily_df.withColumn(
        "daily_sum_is_target_recording",
        F.sum("is_target_recording").over(window_spec),
    )
    .withColumn(
        "daily_sum_is_actual_recording",
        F.sum("is_actual_recording").over(window_spec),
    )
    .filter(
        (F.col("daily_sum_is_target_recording") > 0)
        | (F.col("daily_sum_is_actual_recording") > 0)
    )
    .drop("daily_sum_is_target_recording", "daily_sum_is_actual_recording")
    .withColumn(
        "datetime_logged", F.lit(ts)
    )  # Capture current timestamp for troubleshooting purposes
)

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that late-arriving or updated signal data doesn't result in overlapping or duplicated intervals.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(table_name, start_date, end_date)

# COMMAND ----------

create_or_update_table(
    table_name,
    dashcam_actual_and_target_recording_intervals_daily_df.dropDuplicates(
        ["date", "org_id", "device_id", "start_ms"]
    ),
    "date",
    ["date", "org_id", "device_id", "start_ms"],
)
