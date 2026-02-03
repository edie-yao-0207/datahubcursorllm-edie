# Databricks notebook source
# MAGIC %pip install slack_sdk

# COMMAND ----------

# MAGIC %run /backend/backend/databricks_data_alerts/slack_utility

# COMMAND ----------

import boto3
import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pytz import timezone


def get_scheduled_offboarding_events():
    q = """
SELECT
  msoe.OrgId AS org_id,
  o.name AS org_name,
  DATE(msoe.DeleteAt) AS delete_at
FROM dynamodb.mem_scheduled_offboarding_events msoe
JOIN clouddb.organizations o
ON msoe.OrgId = o.id
ORDER BY delete_at
"""
    scheduled_offboarding_events = spark.sql(q).rdd.collect()
    message_lines = [
        f"*[{boto3.session.Session().region_name}] Upcoming Orgs to be offboarding from MEM*"
    ]
    for event in scheduled_offboarding_events:
        message_lines += [
            f"• <https://cloud.samsara.com/o/{event.org_id}/fleet/mdm|{event.org_name}> ({event.org_id}): {event.delete_at}"
        ]

    if len(scheduled_offboarding_events) == 0:
        message_lines += ["No upcoming Deletions :)"]

    return message_lines


message_lines = get_scheduled_offboarding_events()
slack_client = get_client()

try:
    resp = slack_client.chat_postMessage(
        channel="metrics-mdmsdk", text="\n".join(message_lines)
    )
except SlackApiError as e:
    print(e.response["error"])

# COMMAND ----------
