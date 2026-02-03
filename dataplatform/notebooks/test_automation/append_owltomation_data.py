# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Download data from S3 and load it in to data lake tables - create new tables from scratch.
# MAGIC
# MAGIC This notebook is intended to be run every hour, as defined in the metadata file.
# MAGIC The metadata file defines a Quartz cron job, [as explained in the instructions to setting up an automated notebook](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17756812/Scheduling+a+single+Databricks+notebook+via+the+backend+repo#Step-2%3A-add-metadata-file), and also in the [info on the metadata schema](https://github.com/samsara-dev/backend/blob/master/dataplatform/_json_schemas/notebook_metadata_schema.json#L41-L44).
# MAGIC The notebook can also be run on demand.

# COMMAND ----------

import json
import pandas as pd
import re
from delta.tables import *
from datetime import datetime, timedelta, timezone


# COMMAND ----------

# Get all the filenames in s3 owltomation_results, then filter for just the json files
files = []
folders = dbutils.fs.ls(
    "/Volumes/s3/firmware-test-automation/root/owltomation_results/"
)
for folder in folders:
    files += dbutils.fs.ls(folder.path)

# COMMAND ----------

day_today = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
early_time_datetime = day_today - timedelta(hours=4)
early_time = early_time_datetime.timestamp() * 1000

print(f"Collecting results starting from {early_time_datetime}")

# COMMAND ----------

# Filter based on dates and only json files
# Store only the file path, and remove the "dbfs:" from the beginning of the path
list_of_json_files = [
    file.path[5:]
    for file in files
    if (file.name.endswith(".json") and (file.modificationTime > early_time))
]

print(len(list_of_json_files))

# COMMAND ----------

if len(list_of_json_files) == 0:
    dbutils.notebook.exit("No new results to upload. Exiting.")

# COMMAND ----------

# Lists for Dataframe for separate results
plan_df_list = []
test_df_list = []
step_df_list = []

# COMMAND ----------

for i, obj in enumerate(list_of_json_files):
    if i % 50 == 0:
        print(i)
    try:
        with open(obj, "r") as f:
            json_result = json.load(f)
    except Exception as e:
        print(obj, e)
        continue
    results_dict = {k: v for k, v in json_result.items() if k not in ["tests"]}
    test_plan_id = f'{results_dict["product"]}_{results_dict["test_plan"]}_{results_dict["start_unixms"]}'
    results_dict["test_plan_id"] = test_plan_id
    plan_df_list.append(results_dict)
    # Get test items
    for test in json_result["tests"]:
        test_start = test["start_unixms"]
        # test_id is updated on April 16, 2025 to ensure uniqueness
        if test_start < 1744786800000:
            test_id = f"{test['name']}.{test['queue']}.{test['testbed']}.{test_start}"
        else:
            test_id = (
                f"{test['name']}.{test['test_set']}.{test['testbed']}.{test_start}"
            )

        test_row = {
            "test_plan_id": test_plan_id,
            "test_id": test_id,
            "product": results_dict["product"],
            "test_plan": results_dict["test_plan"],
        }
        for key, val in test.items():
            if key in ["device_id", "org_id"] and val is not None:
                test_row[key] = str(val)
            elif key not in ["steps"]:
                test_row[key] = val
        test_df_list.append(test_row)
        # Get test step items
        for step in test["steps"]:
            step_row = {
                "test_plan_id": test_plan_id,
                "test_id": test_id,
                "test_name": test["name"],
                "test_set_name": test["test_set"],
                "product": results_dict["product"],
                "test_plan": results_dict["test_plan"],
            }
            for key, val in step.items():
                step_row[key] = val
            step_df_list.append(step_row)

# COMMAND ----------

# Dataframe for separate results
plan_df = pd.DataFrame(plan_df_list)
test_df = pd.DataFrame(test_df_list)
step_df = pd.DataFrame(step_df_list)

# COMMAND ----------

# Explicitly define columns to enforce schema and avoid merge errors if new columns are added or missing
plan_df_cols = [
    "passed",
    "test_plan",
    "execution_id",
    "start_unixms",
    "end_ms",
    "duration_ms",
    "datetime",
    "product",
    "firmware_version_build_string",
    "git_hash",
    "git_branch",
    "buildkite_url",
    "buildkite_pipeline",
    "test_plan_id",
    "app_name",
]
test_df_cols = [
    "test_result",
    "testbed",
    "queue",
    "test_set_name",
    "test_name",
    "test_id",
    "start_unixms",
    "datetime",
    "duration_ms",
    "testbed_description",
    "buildkite_url",
    "device_id",
    "device_serial",
    "org_id",
    "test_plan_id",
    "product",
    "test_plan",
    "attempts",
    "notes",
    "jira_ticket",
]
step_df_cols = [
    "name",
    "value",
    "unit",
    "step_result",
    "low_limit",
    "high_limit",
    "info",
    "timestamp_ms",
    "datetime",
    "test_name",
    "test_set_name",
    "test_id",
    "test_plan_id",
    "product",
    "test_plan",
]

# COMMAND ----------


def clean_cols(df, expected_col_list):
    df_cols = df.columns
    for col in expected_col_list:
        if col not in df_cols:
            if col == "attempts":
                # If attempts does not exist, fill with 1
                df[col] = 1
            else:
                df[col] = ""

    return df[expected_col_list]


# COMMAND ----------

# MAGIC %md
# MAGIC Complete plan_df cleanup and upload first, in case test_df and step_df are empty

# COMMAND ----------

plan_df["datetime"] = pd.to_datetime(plan_df["start_unixms"], unit="ms")
plan_df["firmware_version_build_string"] = plan_df["firmware_version"].apply(
    lambda x: x.get("build_string", None) if type(x) == dict else ""
)

# COMMAND ----------

plan_df = clean_cols(plan_df, plan_df_cols)

# COMMAND ----------

plan_df["git_branch"] = plan_df["git_branch"].fillna("")
plan_df["git_hash"] = plan_df["git_hash"].fillna("")
plan_df["app_name"] = plan_df["app_name"].fillna("")

# COMMAND ----------

plan_df_spark = spark.createDataFrame(plan_df)
display(plan_df_spark)

# COMMAND ----------

# plan_df_spark.write.mode("overwrite").saveAsTable("owltomation_results.plan_results")

plan_df_existing = DeltaTable.forName(spark, "owltomation_results.plan_results")

plan_df_existing.alias("current").merge(
    plan_df_spark.alias("updates"),
    "current.test_plan_id = updates.test_plan_id AND current.buildkite_url = updates.buildkite_url",
).whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC Complete test_df and step_df cleanup and upload

# COMMAND ----------

if len(test_df) == 0:
    dbutils.notebook.exit("No test or step results to upload. Exiting.")

# COMMAND ----------

test_df = test_df.rename(
    columns={
        "name": "test_name",
        "test_set": "test_set_name",
        "result": "test_result",
    }
)
step_df = step_df.rename(columns={"result": "step_result"})

# COMMAND ----------

test_df["datetime"] = pd.to_datetime(test_df["start_unixms"], unit="ms")
step_df["datetime"] = pd.to_datetime(step_df["timestamp_ms"], unit="ms")

# COMMAND ----------

step_df["value"] = step_df["value"].astype("str")
step_df["high_limit"] = step_df["high_limit"].astype("str")
step_df["low_limit"] = step_df["low_limit"].astype("str")

# COMMAND ----------

test_df = clean_cols(test_df, test_df_cols)
step_df = clean_cols(step_df, step_df_cols)

# COMMAND ----------

test_df["testbed_description"] = test_df["testbed_description"].fillna("")
test_df["device_id"] = test_df["device_id"].fillna("")
test_df["device_serial"] = test_df["device_serial"].fillna("")
test_df["org_id"] = test_df["org_id"].fillna("")
test_df["notes"] = test_df["notes"].fillna("")
test_df["jira_ticket"] = test_df["jira_ticket"].fillna("")
test_df["attempts"] = test_df["attempts"].fillna(1)
test_df["attempts"] = test_df["attempts"].astype("int")

# COMMAND ----------

test_df_spark = spark.createDataFrame(test_df)
display(test_df_spark)

# COMMAND ----------

step_df_spark = spark.createDataFrame(step_df)
display(step_df_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC Upload tables to delta lake

# COMMAND ----------

# test_df_spark.write.mode("overwrite").saveAsTable("owltomation_results.test_results")

test_df_existing = DeltaTable.forName(spark, "owltomation_results.test_results")

test_df_existing.alias("current").merge(
    test_df_spark.alias("updates"),
    "current.test_plan_id = updates.test_plan_id AND current.test_name = updates.test_name AND current.start_unixms = updates.start_unixms AND current.test_id = updates.test_id AND current.buildkite_url = updates.buildkite_url",
).whenNotMatchedInsertAll().execute()


# COMMAND ----------

# step_df_spark.write.mode("overwrite").saveAsTable("owltomation_results.step_results")

step_df_existing = DeltaTable.forName(spark, "owltomation_results.step_results")

step_df_existing.alias("current").merge(
    step_df_spark.alias("updates"),
    "current.test_plan_id = updates.test_plan_id AND current.test_name = updates.test_name AND current.name = updates.name AND current.timestamp_ms = updates.timestamp_ms AND current.test_id = updates.test_id",
).whenNotMatchedInsertAll().execute()

# COMMAND ----------
