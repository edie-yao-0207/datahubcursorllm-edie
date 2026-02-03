# Databricks notebook source

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import time

import boto3
import datadog

# Get Datadog keys from parameter store and initialize datadog
ssm_client = get_ssm_client("standard-read-parameters-ssm")
api_key = get_ssm_parameter(ssm_client, "DATADOG_API_KEY")
app_key = get_ssm_parameter(ssm_client, "DATADOG_APP_KEY")
datadog.initialize(api_key=api_key, app_key=app_key)

region = boto3.session.Session().region_name
print(f"region: {region}")

# Calculations to get difference in time between now and most recent row
current = int(round(time.time() * 1000))
df = spark.sql(
    "select max(value.received_at_ms) as time from kinesisstats.location where date(date) = current_date() or date(date) = date_sub(current_date(), 1)"
)
most_recent = df.collect()[0]["time"]
difference = current - most_recent

print(
    f"current: {current}, \t which is: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current / 1000))}"
)
print(
    f"most_recent: {most_recent}, \t which is: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(most_recent / 1000))}"
)
print(
    f"difference: {difference}, \t\t which is: {int(difference / 3600000)}h {int(difference % 3600000 / 60000)}m"
)
print(f"If the difference is more than 6 hours, the oncall engineer will be paged")

# Send the metric to datadog
datadog.api.Metric.send(
    metric="ksdeltalake.ms_since_last_update",
    points=[(time.time(), difference)],
    type="gauge",
    tags=["table:location", f"region:{region}"],
)

print(f"Metric sent to Datadog")
