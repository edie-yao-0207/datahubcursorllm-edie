# Databricks notebook source
# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import re
import boto3
import tempfile
import json
import numpy as np
import pandas as pd
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from datetime import date, datetime, time, timedelta

client = get_s3_client("samsara-firmware-test-automation-read")
all_files = client.list_objects(Bucket="samsara-firmware-test-automation")
file_contents = all_files["Contents"]


paginator = client.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket="samsara-firmware-test-automation")
finalist = []

for page in paginator.paginate(Bucket="samsara-firmware-test-automation"):
    for obj in page["Contents"]:
        if "owltomation_results" in obj["Key"]:
            finalist.append(obj["Key"])


df = pd.DataFrame({"test_list": finalist})
df = df["test_list"].str.split("/", n=3, expand=True)
df.columns = ["folder1", "folder", "filename"]
df["filename"] = df["filename"].str.replace(".json", "")

df.drop(["folder1"], axis=1, inplace=True)
df.dropna(subset=["filename"], axis=0, inplace=True)

# Function to extract parts of the string Looks for name-date-time-millisecond format
def extract_parts(s):
    match = re.match(r"(.+?)_(\d{4}-\d{2}-\d{2})_(\d{2}h\d{2}m\d{2}s)(?:_(\d+))?", s)
    if match:
        return match.groups()
    return None


# Apply the function and create new columns
df[["name", "date", "time", "millisecond"]] = df["filename"].apply(
    lambda x: pd.Series(extract_parts(x))
)

df.drop(["filename"], axis=1, inplace=True)

sdf = spark.createDataFrame(df)
sdf = sdf.withColumn("date", to_date(sdf["date"], "yyyy-MM-dd"))

exist_owlt = DeltaTable.forName(spark, "hardware_analytics.owltomation_tests")
exist_owlt.alias("original").merge(
    sdf.alias("updates"),
    "original.folder = updates.folder \
    and original.name = updates.name \
    and original.date = updates.date \
    and original.time = updates.time \
    and original.millisecond = updates.test_type",
).whenNotMatchedInsertAll().execute()
