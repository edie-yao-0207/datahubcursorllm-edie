# Databricks notebook source
# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import boto3
import json
import datetime

s3client = get_s3_client("samsara-s3-inventory-read")

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

# Pulling manifest file from the day before
response = s3client.get_object(
    Bucket="samsara-s3-inventory",
    Key=f"samsara-dashcam-videos/entire-bucket-daily-parquet/{yesterday}T00-00Z/manifest.json",
)
body_in_json = response["Body"].read().decode("utf-8")
json_response = json.loads(body_in_json)

file_names = [f"s3://samsara-s3-inventory/{f['key']}" for f in json_response["files"]]

# Load all files into table
df = spark.read.parquet(*file_names)
df.write.format("delta").mode("overwrite").saveAsTable(
    "playground.dashcam_videos_s3_inventory"
)
