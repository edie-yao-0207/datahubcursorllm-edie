# Databricks notebook source
# MAGIC %md
# MAGIC ## Alerting System
# MAGIC This notebook shows how to use the regular alerting system when we know what threshold we want to use. This notebook sets up alerting for recording uptime, and fires off an alert if recording uptime went below 99% any day in the last two weeks.
# MAGIC
# MAGIC For more info: https://paper.dropbox.com/doc/Databricks-Alerting-System--A9biFapyb5~thzd43dE3cfCJAg-ocMHGCVEDw7MaA4c67HJz

# COMMAND ----------

# MAGIC %md
# MAGIC Import the system

# COMMAND ----------

# MAGIC %run backend/backend/databricks_data_alerts/alerting_system

# COMMAND ----------

# MAGIC %md
# MAGIC Fetch recording uptime by day for the last two weeks

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view latest_recording_uptime as (
# MAGIC   select
# MAGIC     date,
# MAGIC     sum(total_trip_grace_recording_duration_ms) / sum(total_trip_connected_duration_ms) as recording_uptime
# MAGIC   from
# MAGIC     dataprep_safety.cm_device_health_daily
# MAGIC   where
# MAGIC     date >= date_sub(current_date(), 14)
# MAGIC   group by
# MAGIC     date
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the alert

# COMMAND ----------

latest_recording_uptime = spark.table("latest_recording_uptime")
below_threshold_uptime = latest_recording_uptime.filter(
    latest_recording_uptime.recording_uptime < 0.99
)  # There should be no rows in this dataframe (we want recording uptime to be >= .99 every day)
execute_alert(
    below_threshold_uptime,
    "mathew.calmer@samsara.com",
    "safety_fw_automated_data_alerts",
    "Recording_Uptime_Alert",
    "Recording uptime went below .99 in the past two weeks",
)  # Replace with your details
