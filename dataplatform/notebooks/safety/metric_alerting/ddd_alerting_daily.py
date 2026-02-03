# Databricks notebook source
# MAGIC %md
# MAGIC ## DDD Alerting
# MAGIC This is a notebook showing how to use the metric alerting system to track normalized DDD rate. The way the system works is simple: we first identify a period of "good data" and train the system on that. Then we feed in the data we would like to evaluate and if the data differs from the system's predictions, we can send an alert to a slack channel.
# MAGIC
# MAGIC This notebook is intended to be setup as job. On each run it will train the model then alert on the most recent data if recent data deviates from what the model expects.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.broadcastTimeout=100000

# COMMAND ----------

# MAGIC %md Start by importing the system

# COMMAND ----------

# MAGIC %run "backend/backend/metric_forecast_alerts/metric_forecast_alerts"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Historical Data
# MAGIC The timestamp column must be called ds, and the value column must be called y. These should be the only two columns in the dataframe.

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace temporary view historical_data as (
# MAGIC   select
# MAGIC     date as ds,
# MAGIC     count(*) as y
# MAGIC   from
# MAGIC     safetydb_shards.safety_events as accel
# MAGIC   where
# MAGIC     accel.detail_proto.accel_type = 13 and
# MAGIC     accel.date >= date_sub(current_date(), 50) and
# MAGIC     accel.date <= date_sub(current_date(), 18)
# MAGIC   group by
# MAGIC     date
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build The Model
# MAGIC When we instantiate the alerter we pass in the historical data, and the constructor will train the model. We also pass in the channel we'd like to send slack alerts to, and the name of the alert. For a full list of options, look here: https://paper.dropbox.com/doc/Time-Series-Anomaly-Detection-Alerts--A9Zuz0HUn1mpf5635JegVqghAg-r7wxhbMTTPJVbAOauJVUu

# COMMAND ----------

historical_sdf = spark.table("historical_data")
alerter = MetricForecastAlert(
    historical_sdf,
    ["safety_fw_automated_data_alerts"],
    "Daily DDD Count",
    weekly_seasonality=True,
    filter_holidays=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup The Data To Evaluate
# MAGIC We want to alert on the most recent data, so we fetch the last two weeks

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace temporary view current_data as (
# MAGIC   select
# MAGIC     date as ds,
# MAGIC     count(*) as y
# MAGIC   from
# MAGIC     safetydb_shards.safety_events as accel
# MAGIC   where
# MAGIC     accel.detail_proto.accel_type = 13 and
# MAGIC     accel.date >= date_sub(current_date(), 18) and
# MAGIC     accel.date <= date_sub(current_date(), 1)
# MAGIC   group by
# MAGIC     date
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Data

# COMMAND ----------

current_data = spark.table("current_data")

# COMMAND ----------

# We can view the forecast and see how the data deviates
alerter.show_forecast_with_current_data(current_data)

# COMMAND ----------

# We then alert on the data. Generally we want to use the prediction threshold alert type
alerter.run_and_alert(
    current_data,
    alert_type="prediction_threshold",
    alert_threshold=2,
    model_threshold=0.20,
)

# COMMAND ----------
