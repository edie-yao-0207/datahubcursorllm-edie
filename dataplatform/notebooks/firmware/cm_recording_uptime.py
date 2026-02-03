# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)

# COMMAND ----------

table_name = "dataprep_firmware.cm_recording_uptime"

# COMMAND ----------

cm_recording_intervals_df = spark.sql(
    """--sql
    SELECT
        date
        , org_id
        , device_id
        , start_ms
        , end_ms
        , is_target_recording
        , is_actual_recording
        , recording_reason
    FROM
        dataprep_firmware.cm_actual_and_target_recording_intervals_daily
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
)

# Only retrieve CMs with a firmware version that has Recording State Manager (RSM) enabled
valid_firmware_df = spark.sql(
    """--sql
    SELECT
        date
        , org_id
        , cm_device_id
        , vg_device_id
    FROM 
        dataprep_firmware.cm_uptime_eligible_devices
    WHERE
        date >= '{}'
        AND date <= '{}'
    --endsql
    """.format(
        start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
)

# COMMAND ----------

# Retrieve only those CMs with a firmware version that has Recording State Manager (RSM) enabled
valid_cm_recording_intervals_df = (
    cm_recording_intervals_df.alias("a")
    .join(
        valid_firmware_df.alias("b"),
        (F.col("a.date") == F.col("b.date"))
        & (F.col("a.org_id") == F.col("b.org_id"))
        & (F.col("a.device_id") == F.col("b.cm_device_id")),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.device_id",
        "a.start_ms",
        "a.end_ms",
        "a.is_target_recording",
        "a.is_actual_recording",
        "a.recording_reason",
    )
)

cm_target_recording_agg_df = (
    valid_cm_recording_intervals_df.filter(F.col("is_target_recording") == 1)
    .groupBy("date", "org_id", "device_id")
    .agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("target_recording_ms"))
)

# There are intervals of time where is_target_recording=0 but is_actual_recording=1.
# It appears these are transitory periods where the camera is starting up but not actually recording.
# Therefore, only count recording periods as those during which is_target_recording=is_actual_recording=1.
# Example device shows such a period on 2024-12-18 10:05:55 to 2024-12-18 10:07:37
# https://cloud.eu.samsara.com/o/562949953423919/devices/844424930727338/camera_debug?image=0&assetMs=1736419410514&duration=6021&end_ms=1734517178181
cm_actual_recording_agg_df = (
    valid_cm_recording_intervals_df.filter(
        (F.col("is_actual_recording") == 1) & (F.col("is_target_recording") == 1)
    )
    .groupBy("date", "org_id", "device_id")
    .agg(
        # Coalesce handles NULL from empty sum (when a CM exists but has no intervals in the group)
        F.coalesce(F.sum(F.col("end_ms") - F.col("start_ms")), F.lit(0)).alias(
            "actual_recording_ms"
        )
    )
)

# COMMAND ----------

# Capture a single consistent timestamp for the transformation,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across partitions.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

cm_recording_uptime_df = (
    cm_target_recording_agg_df.alias("a")
    .join(
        cm_actual_recording_agg_df.alias("b"),
        ["date", "org_id", "device_id"],
        how="left",
    )
    .withColumn(
        "recording_uptime",
        # target_recording_ms and actual_recording_ms have 8 significant digits
        # When LEFT JOIN finds no actual recording data, coalesce converts NULL to 0,
        # resulting in 0/denominator = 0 (zero recording uptime, not unknown).
        F.round(
            F.coalesce(F.col("b.actual_recording_ms"), F.lit(0))
            / F.col("a.target_recording_ms"),
            8,
        ),
    )
    .select(
        "a.date",
        "a.org_id",
        "a.device_id",
        "a.target_recording_ms",
        "b.actual_recording_ms",
        "recording_uptime",
    )
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
    cm_recording_uptime_df,
    "date",
    ["date", "org_id", "device_id"],
)
