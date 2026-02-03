# Databricks notebook source
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth


def get_jira_info(key):
    url = "https://samsaradev.atlassian.net/rest/api/3/issue/" + key
    headers = {"Accept": "application/json"}
    query = {"query": "query"}
    jiraUsername = dbutils.secrets.get(scope="jira", key="username")
    jiraPassword = dbutils.secrets.get(scope="jira", key="password")
    auth = HTTPBasicAuth(jiraUsername, jiraPassword)
    response = requests.get(url, headers=headers, auth=auth)
    json_info = response.json()
    if json_info["fields"]["assignee"] is not None:
        assignee = json_info["fields"]["assignee"]["displayName"]
    else:
        assignee = None
    jira_dict = {
        "key": json_info["key"],
        "summary": json_info["fields"]["summary"],
        "type": json_info["fields"]["issuetype"]["name"],
        "status": json_info["fields"]["status"]["name"],
        "assignee": assignee,
        "links": [
            x[list(x)[-1]]["key"] + ": " + x[list(x)[-1]]["fields"]["summary"]
            for x in json_info["fields"]["issuelinks"]
        ],
    }
    return jira_dict


def create_jira_df(keys):
    dict_list = []
    for key in keys:
        try:
            info = get_jira_info(key)
            dict_list.append(info)
        except:
            print("could not get info for " + key)
    existing_df = spark.table("hardware_analytics.field_hardware_jira_tickets")
    existing_schema = existing_df.schema
    df = spark.createDataFrame(dict_list, existing_schema)
    return df


# COMMAND ----------

# Get jiras from sn_to_jira tables
jira_list = spark.sql(
    f"""
                      select distinct jira from hardware_analytics.sn_to_jira_priority_rma 
                      union select distinct jira from hardware_analytics.sn_to_jira_cfe where jira is not null
                      union select distinct jira from hardware_analytics.sn_to_jira_rma where jira is not null
                      union select distinct screening_result as jira from hardware.initial_screening_results where screening_result like 'HWQ%'
                      union select distinct signature_key as jira from hardware_analytics.field_signatures"""
)
jira_list_df = jira_list.toPandas()
fixed_list = [x.split(":")[0] for x in list(set(jira_list_df["jira"]))]
jira_df = create_jira_df(fixed_list)
jira_df = jira_df.dropDuplicates(["key"])

# COMMAND ----------

from delta.tables import *

existing_table = DeltaTable.forName(
    spark, "hardware_analytics.field_hardware_jira_tickets"
)
existing_table.alias("original").merge(
    jira_df.alias("updates"),
    "original.key = updates.key",
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
