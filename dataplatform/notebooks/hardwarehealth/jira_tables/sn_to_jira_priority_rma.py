# Databricks notebook source
import requests
import re
import pandas as pd
from delta.tables import *
from requests.auth import HTTPBasicAuth
from pyspark.sql.functions import current_timestamp, split, explode, trim, col, lit


def is_serial(string):
    if (
        re.match("^[A-Za-z0-9]{4}-[A-Za-z0-9]{3}-[A-Za-z0-9]{3}$", string)
        or re.match("^[A-Za-z0-9]{12}$", string)
        or re.match("^[A-Za-z0-9]{11}$", string)
        or re.match("^[A-Za-z0-9]{10}$", string)
    ):
        return True
    else:
        return False


# Function to query jira via the API and get list of serial on priority RMA tickets
def get_priority_rma_serials(auth):
    filter_url = "https://samsaradev.atlassian.net/rest/api/3/search?jql=filter=15037"
    headers = {"Accept": "application/json"}
    query = {"query": "query"}
    response = requests.get(filter_url, headers=headers, auth=auth)
    json_info = response.json()
    all_rows = []
    for issue in json_info["issues"]:
        for row in issue["fields"]["customfield_10955"]["content"]:
            if row["type"] == "paragraph":
                for content in row["content"]:
                    if content["type"] == "text":
                        serial = (
                            content["text"].replace("-", "").replace(" ", "").upper()
                        )
                        if is_serial(serial):
                            all_rows.append([serial, issue["key"]])
            elif row["type"] == "table":
                for t_row in row["content"]:
                    serial = (
                        t_row["content"][0]["content"][0]["content"][0]["text"]
                        .replace("-", "")
                        .replace(" ", "")
                        .upper()
                    )
                    if is_serial(serial):
                        all_rows.append([serial, issue["key"]])
    return all_rows


# COMMAND ----------

# Use secrets to get jira auth
jiraUsername = dbutils.secrets.get(scope="jira", key="username")
jiraPassword = dbutils.secrets.get(scope="jira", key="password")
auth = HTTPBasicAuth(jiraUsername, jiraPassword)

# COMMAND ----------

# Get Priority RMA tickets
all_rows = get_priority_rma_serials(auth)
schema = ["serial", "jira"]
prio_rma_df = spark.createDataFrame(all_rows, schema)
prio_rma_df = prio_rma_df.withColumn(
    "added_by", lit("automated_from_priority_rma_ticket")
)
prio_rma_df = prio_rma_df.withColumn("date_added", current_timestamp())

# COMMAND ----------

# Update table only with new entries
existing_table = DeltaTable.forName(spark, "hardware_analytics.sn_to_jira_priority_rma")
existing_table.alias("original").merge(
    prio_rma_df.alias("updates"),
    "original.serial = updates.serial and original.jira = updates.jira",
).whenNotMatchedInsertAll().execute()

# COMMAND ----------
