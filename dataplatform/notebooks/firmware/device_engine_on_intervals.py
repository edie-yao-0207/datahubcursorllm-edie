# Databricks notebook source
# MAGIC %run ./helpers

# COMMAND ----------

start_date, end_date = get_start_end_date(8, 1)
prev_value_lookback_days = 7
query_start_ms = to_ms(start_date)
query_end_ms = to_ms(end_date) + day_ms() - 1

# COMMAND ----------

table_name = "dataprep_firmware.device_engine_on_intervals"

# COMMAND ----------

osdenginestate_df = spark.sql(
    """--sql
    SELECT *
    FROM kinesisstats_history.osdenginestate
    WHERE
        date >= DATE_SUB('{}', {})
        AND date <= '{}'
    --endsql
    """.format(
        start_date, prev_value_lookback_days, end_date
    )
)

# Retrieve heartbeats in order to properly terminate device engine on intervals
heartbeats_df = spark.sql(
    """--sql
    SELECT * FROM dataprep.device_heartbeats_extended
    --endsql
    """
)

# COMMAND ----------

# Engine State values:
# 0 = Engine State Off
# 1 = Engine State On (Running)
# 2 = Engine State Idle
# go/src/samsaradev.io/fleet/fuel/fuelhelpers/engine_state.go
engine_intervals_df = create_intervals_v2(
    osdenginestate_df, lambda x: x == 1 or x == 2, query_start_ms, query_end_ms
)

# End intervals used in question at the last heartbeat.
# This is to prevent the edge case where a device continues to emit an objectstat even though heartbeats have stopped.
engine_intervals_hb_df = terminate_intervals_at_last_hb(
    engine_intervals_df, heartbeats_df
)

# Capture a single consistent timestamp for the entire job,
# rather than evaluating current_timestamp() separately for each row,
# which could produce slightly different values across partitions.
ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

engine_intervals_daily_df = create_intervals_daily(
    engine_intervals_hb_df, start_date, end_date
).withColumn(
    "datetime_logged", F.lit(ts)
)  # Capture current timestamp for troubleshooting purposes

# COMMAND ----------

# Calculate total engine on duration per device per day
engine_intervals_daily_agg_df = engine_intervals_daily_df.groupBy(
    "date", "org_id", "device_id"
).agg(F.sum(F.col("end_ms") - F.col("start_ms")).alias("engine_on_ms"))

# Assert that no device exceeds 24 hours of engine on time per day
max_duration_hours = engine_intervals_daily_agg_df.select(
    F.max(F.col("engine_on_ms") / (1000 * 60 * 60))
).collect()[0][0]
assert (
    max_duration_hours <= 24.0
), f"Found device with {max_duration_hours:.2f} hours of engine on time in a single day"

# COMMAND ----------

# Delete any existing rows for the specified date range before inserting updated data.
# This ensures that late-arriving or updated signal data doesn't result in overlapping or duplicated intervals.
# The deletion is scoped by `date` to allow safe, idempotent updates without full table recomputation.
delete_entries_by_date(table_name, start_date, end_date)

# COMMAND ----------

create_or_update_table(
    table_name,
    engine_intervals_daily_df.filter(F.col("date") >= start_date),
    "date",
    ["date", "org_id", "device_id", "start_ms"],
)
