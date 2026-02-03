# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)
# Device powerstate may not change for a long time. This lookback is set to ensure
# we capture the previous state for devices that may not report state changes frequently.
prev_value_lookback_days = 90
query_start_ms = to_ms(start_date)
query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

table_name = "dataprep_firmware.device_full_power_intervals_v3"

# COMMAND ----------

osdpowerstate_df = spark.sql(
    """--sql
    SELECT a.*
    FROM kinesisstats_history.osdpowerstate as a
    WHERE
        a.date >= date_sub('{}', {})
        AND a.date <= '{}'
    --endsql
    """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

osdpowerstate_df = osdpowerstate_df.withColumn(
    "power_state",
    F.when(
        F.col("value.int_value").isNull(),
        F.col("value.int_value").cast("long"),
    ).otherwise(F.col("value.int_value")),
)

# Retrieve heartbeats in order to properly terminate device full power intervals
heartbeats_df = spark.sql(
    """--sql
    SELECT * FROM dataprep.device_heartbeats_extended
    --endsql
    """
)

# COMMAND ----------

power_intervals_df = create_intervals_v2(
    osdpowerstate_df, lambda x: x == 1 or x == 7, query_start_ms, query_end_ms
)

# End intervals used in question at the last heartbeat.
# This is to prevent the edge case where a device continues to emit an objectstat even though heartbeats have stopped.
power_intervals_hb_df = terminate_intervals_at_last_hb(
    power_intervals_df, heartbeats_df
)

# Capture a single consistent timestamp for the entire job,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across partitions.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

power_intervals_daily_df = create_intervals_daily(
    power_intervals_hb_df, start_date, end_date
).withColumn(
    "datetime_logged", F.lit(ts)
)  # Capture current timestamp for troubleshooting purposes

# COMMAND ----------

# Calculate total full power duration per device per day
power_intervals_daily_agg_df = power_intervals_daily_df.groupBy(
    "date", "org_id", "device_id"
).agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("vg_full_power_ms"))

# Assert that no device exceeds 24 hours of full power time per day
max_duration_hours = power_intervals_daily_agg_df.select(
    F.max(F.col("vg_full_power_ms") / (1000 * 60 * 60))
).collect()[0][0]
assert (
    max_duration_hours <= 24.0
), f"Found device with {max_duration_hours:.2f} hours of full power time in a single day"

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that late-arriving or updated signal data doesn't result in overlapping or duplicated intervals.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(table_name, start_date, end_date)

# COMMAND ----------

create_or_update_table(
    table_name,
    power_intervals_daily_df.filter(F.col("date") >= start_date),
    "date",
    ["date", "org_id", "device_id", "start_ms"],
)
