# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# COMMAND ----------

table_name = "dataprep_firmware.cm_final_uptime"

# COMMAND ----------

# Capture a single consistent timestamp for the transformation,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across rows.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

cm_final_uptime_df = spark.sql(
    """--sql
    SELECT
        cu.date
        , cu.org_id
        , cu.device_id
        , dd.product_name
        , cu.vg_full_power_ms
        , cu.cm_full_power_connected_ms
        , cu.count_vg_device_id
        , cu.cm_uptime
        , cu.vg_engine_on_ms
        , cu.cm_connected_engine_on_ms
        , cu.count_vg_device_id_engine_on
        , cu.cm_uptime_engine_on
        , ru.actual_recording_ms
        , ru.target_recording_ms
        , ru.recording_uptime
        , CASE
            WHEN cu.cm_uptime IS NULL THEN NULL
            -- A VG can be powered and connected to the CM, while no target or actual 
            -- recording state signals are being sent.
            -- To avoid zeroing out final_uptime for these cases, set final_uptime = cm_uptime.
            WHEN ru.recording_uptime IS NULL THEN ROUND(cu.cm_uptime, 8)
            -- Multiply recording uptime by connection uptime to get the final uptime metric
            ELSE ROUND(ru.recording_uptime * cu.cm_uptime, 8)
        END AS final_uptime
        , CASE
            WHEN cu.cm_uptime_engine_on IS NULL THEN NULL
            -- Similar logic for engine on final uptime
            WHEN ru.recording_uptime IS NULL THEN ROUND(cu.cm_uptime_engine_on, 8)
            ELSE ROUND(ru.recording_uptime * cu.cm_uptime_engine_on, 8)
        END AS final_uptime_engine_on
    FROM
        dataprep_firmware.cm_uptime cu
    LEFT JOIN
        dataprep_firmware.cm_recording_uptime ru
        ON cu.date = ru.date
        AND cu.org_id = ru.org_id
        AND cu.device_id = ru.device_id
    LEFT JOIN
        datamodel_core.dim_devices dd
        ON cu.date = dd.date
        AND cu.org_id = dd.org_id
        AND cu.device_id = dd.device_id
    WHERE
        cu.date >= '{}'
        AND cu.date <= '{}'
    --endsql
    """.format(
        start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
).withColumn(
    "datetime_logged", F.lit(ts)
)  # Capture current timestamp for troubleshooting purposes

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that data accounts for late-arriving or updated signal data.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(table_name, start_date, end_date)

create_or_update_table(
    table_name,
    cm_final_uptime_df,
    "date",
    ["date", "org_id", "device_id"],
)
