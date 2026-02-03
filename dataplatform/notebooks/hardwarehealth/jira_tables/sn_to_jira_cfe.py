# Databricks notebook source
import requests
import re
import pandas as pd
from delta.tables import *
from requests.auth import HTTPBasicAuth
from pyspark.sql.functions import (
    current_timestamp,
    split,
    explode,
    trim,
    col,
    lit,
    coalesce,
)


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


# COMMAND ----------

# Function to query jira via the API and get list of serials on CFE tickets
def get_CFE_serials(auth):
    filter_url = "https://samsaradev.atlassian.net/rest/api/3/search?jql=filter=17580"
    headers = {"Accept": "application/json"}
    query = {"query": "query"}
    response = requests.get(filter_url, headers=headers, auth=auth)
    json_info = response.json()
    all_rows = []
    for issue in json_info["issues"]:
        for row in issue["fields"]["description"]["content"]:
            if row["type"] == "paragraph":
                for content in row["content"]:
                    if content["type"] == "text":
                        if "Serial Number" in content["text"]:
                            serial_options = content["text"].split(" ")
                            for s in serial_options:
                                if is_serial(s):
                                    serial = s.replace("-", "").upper()
                                    all_rows.append([serial, issue["key"]])
    return all_rows


# COMMAND ----------

# Use secrets to get jira auth
jiraUsername = dbutils.secrets.get(scope="jira", key="username")
jiraPassword = dbutils.secrets.get(scope="jira", key="password")
auth = HTTPBasicAuth(jiraUsername, jiraPassword)

# COMMAND ----------

# Get CFE tickets
all_rows = get_CFE_serials(auth)
schema = ["serial", "jira"]
cfe_rma_df = spark.createDataFrame(all_rows, schema)
cfe_rma_df = cfe_rma_df.withColumn("added_by", lit("automated_from_cfe_ticket"))

# COMMAND ----------

# Get CFE tickets from the warranty exchange table (some are linked this way instead of directly)
cfe_tix = spark.sql(
    f"""select serial, trim(jira) as jira from
(select serial, explode(split(custom_jira_key, ',')) as jira from hardware.hardware_exchanges_sf
where custom_jira_key like '%CFE%')
where jira like '%CFE%'"""
)
cfe_tix = cfe_tix.withColumn("added_by", lit("automated_from_zendesk_ticket"))

# COMMAND ----------

# Perform the outer join on 'serial' and 'jira' columns
joined_cfe = cfe_rma_df.join(cfe_tix, on=["serial", "jira"], how="outer")


# Use coalesce to select 'added_by' from df1 if available, otherwise use 'added_by' from df2
all_cfe = joined_cfe.select(
    joined_cfe["serial"],
    joined_cfe["jira"],
    coalesce(cfe_rma_df["added_by"], cfe_tix["added_by"]).alias("added_by"),
)

all_cfe = all_cfe.withColumn("date_added", current_timestamp())


# COMMAND ----------

# Update table only with new entries
existing_table = DeltaTable.forName(spark, "hardware_analytics.sn_to_jira_cfe")
existing_table.alias("original").merge(
    all_cfe.alias("updates"),
    "original.serial = updates.serial and original.jira = updates.jira",
).whenNotMatchedInsertAll().execute()
