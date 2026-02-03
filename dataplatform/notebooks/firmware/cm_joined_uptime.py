# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# COMMAND ----------

database_name = "dataprep_firmware"
table_name = "cm_joined_uptime"
table_suffix = ""  # Table suffix is used during development to version tables

# COMMAND ----------

final_uptime_df = spark.sql(
    """--sql
    SELECT
        date
        , org_id
        , device_id
        , product_name
        , vg_full_power_ms
        , cm_full_power_connected_ms
        , count_vg_device_id
        , cm_uptime
        , vg_engine_on_ms
        , cm_connected_engine_on_ms
        , count_vg_device_id_engine_on
        , cm_uptime_engine_on
        , actual_recording_ms
        , target_recording_ms
        , recording_uptime
        , final_uptime
        , final_uptime_engine_on
    FROM
        dataprep_firmware.cm_final_uptime
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
)

# COMMAND ----------

# Capture a single consistent timestamp for the transformation,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across rows.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

# Use final_uptime_df directly (firmware_trip_recording_uptime dependency removed)
joined_uptime_df = final_uptime_df.select(
    F.col("date"),
    F.col("org_id"),
    F.col("device_id"),
    F.col("product_name").alias("product_name_final"),
    F.col("vg_full_power_ms"),
    F.col("cm_full_power_connected_ms"),
    F.col("count_vg_device_id"),
    F.col("cm_uptime"),
    F.col("vg_engine_on_ms"),
    F.col("cm_connected_engine_on_ms"),
    F.col("count_vg_device_id_engine_on"),
    F.col("cm_uptime_engine_on"),
    F.col("actual_recording_ms"),
    F.col("target_recording_ms"),
    F.col("recording_uptime"),
    F.col("final_uptime"),
    F.col("final_uptime_engine_on"),
).withColumn(
    "datetime_logged", F.lit(ts)
)  # Capture current timestamp for troubleshooting purposes

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that data accounts for late-arriving or updated signal data.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(
    f"{database_name}.{table_name}{table_suffix}", start_date, end_date
)

create_or_update_table(
    f"{database_name}.{table_name}{table_suffix}",
    joined_uptime_df,
    "date",
    ["date", "org_id", "device_id"],
)

# Remove references to dataframes that have now been written to tables so that
# so that the references can be reused without risk of memory or reference leakage
del joined_uptime_df

# COMMAND ----------

joined_uptime_df = spark.sql(
    """--sql
    SELECT *
    FROM {}
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        f"{database_name}.{table_name}{table_suffix}",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

# COMMAND ----------

# Validation: Count devices with final_uptime
total_final_uptime_count = (
    joined_uptime_df.filter(F.col("final_uptime").isNotNull())
    .select("date", "org_id", "device_id")
    .distinct()
    .count()
)

display(f"Total devices with final_uptime: {total_final_uptime_count}")

# COMMAND ----------

dashcam_recording_reason_counts_df = spark.sql(
    """--sql
    SELECT
        date
        , org_id
        , object_id AS device_id
        , MIN(time) AS first_recording
        , MAX(time) AS last_recording
        , COUNT(time) AS count_recording
    FROM
        kinesisstats_history.osddashcamstate
    WHERE
        date >= '{}' AND date <= '{}'
        AND value.is_end IS FALSE
        AND value.is_databreak IS FALSE
        -- A non-null recording_reason indicates that the device was actively recording
        AND value.proto_value.dashcam_state.recording_reason IS NOT NULL
    GROUP BY
        date
        , org_id
        , object_id
    --endsql
    """.format(
        start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
)

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that data accounts for late-arriving or updated signal data.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(
    f"{database_name}.dashcam_recording_reason_counts{table_suffix}",
    start_date,
    end_date,
)

create_or_update_table(
    f"{database_name}.dashcam_recording_reason_counts{table_suffix}",
    dashcam_recording_reason_counts_df,
    "date",
    ["date", "org_id", "device_id"],
)

# Remove references to dataframes that have now been written to tables so that
# so that the references can be reused without risk of memory or reference leakage
del dashcam_recording_reason_counts_df

# COMMAND ----------

dashcam_recording_reason_counts_df = spark.sql(
    """--sql
    SELECT *
    FROM {}
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        f"{database_name}.dashcam_recording_reason_counts{table_suffix}",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )
)

# COMMAND ----------

dashcam_count = (
    dashcam_recording_reason_counts_df.select("date", "org_id", "device_id")
    .distinct()
    .count()
)

# Start with dashcam_recording_reason_counts_df and LEFT JOIN joined_uptime_df onto it
# Then count how many records have final_uptime IS NOT NULL
uptime_count = (
    dashcam_recording_reason_counts_df.select("date", "org_id", "device_id")
    .distinct()
    .join(
        joined_uptime_df.select("date", "org_id", "device_id", "final_uptime"),
        on=["date", "org_id", "device_id"],
        how="left",
    )
    .filter(F.col("final_uptime").isNotNull())
    .select("date", "org_id", "device_id")
    .distinct()
    .count()
)

# Handle divide by zero case - if no devices were actively recording, this is an error condition
if dashcam_count == 0:
    raise ValueError(
        "No devices were actively recording in the specified date range. Cannot calculate coverage."
    )

coverage = uptime_count / dashcam_count
assert coverage > 0.95, f"Ratio is {coverage:.4f}, expected > 0.95"

# Display the share even if the assertion passes
display(f"Ratio is {coverage:.4f}, expected > 0.95")
