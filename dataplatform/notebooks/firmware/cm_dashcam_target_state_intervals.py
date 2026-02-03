# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 0)
query_start_ms = to_ms(start_date)
query_end_ms = to_ms(end_date) + day_ms() - 1
prev_value_lookback_days = 14

# COMMAND ----------

table_name = "dataprep_firmware.cm_dashcam_target_state_intervals"

# COMMAND ----------

osddashcamtargetstate_df = spark.sql(
    """
    select a.*
    from kinesisstats_history.osddashcamtargetstate as a
    join dataprep_firmware.data_ingestion_high_water_mark as hwm
    where a.date >= date_sub('{}', {})
        and a.date <= '{}'
        and a.time <= hwm.time_ms
    """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

osddashcamtargetstate_df = osddashcamtargetstate_df.withColumn(
    "recording_reason",
    F.when(
        F.col("value.proto_value.dashcam_target_state.recording_reason").isNull(),
        F.array().cast("array<integer>"),
    ).otherwise(F.col("value.proto_value.dashcam_target_state.recording_reason")),
)

# Capture a single consistent timestamp for the entire job,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across partitions.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

dashcam_target_state_intervals_df = (
    create_intervals_with_state_value(
        osddashcamtargetstate_df,
        "recording_reason",
        query_start_ms,
        query_end_ms,
    )
    .withColumnRenamed("state_value", "recording_reason")
    .withColumn("datetime_logged", F.lit(ts))
)  # Capture current timestamp for troubleshooting purposes

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that late-arriving or updated signal data doesn't result in overlapping or duplicated intervals.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(table_name, start_date, end_date)

# COMMAND ----------

create_or_update_table(
    table_name,
    dashcam_target_state_intervals_df.filter(F.col("date") >= start_date),
    "date",
    ["date", "org_id", "device_id", "start_ms"],
)
