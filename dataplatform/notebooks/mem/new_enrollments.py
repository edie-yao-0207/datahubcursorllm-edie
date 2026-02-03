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


def get_date_x_days_ago(days_ago):
    today = datetime.date.today()
    week_ago = today - datetime.timedelta(days=days_ago)
    return week_ago


def get_latest_enrollments(start_date, end_date):
    query = f"""
    select
      org_id,
      enterprise.id,
      organizations.name,
      enterprise.created_at
    from mdmdb_shards.enterprise
    join clouddb.organizations on org_id = organizations.id
    where enterprise.created_at >= "{start_date}" and enterprise.created_at < "{end_date}"
    order by enterprise.created_at
  """

    enrollments = spark.sql(query).rdd.collect()

    message_lines = [
        f"*[{boto3.session.Session().region_name}] New Enterprise Enrollments between {start_date} and {end_date}*"
    ]
    for enrollment in enrollments:
        created_at_str = enrollment.created_at.astimezone(
            timezone("America/Los_Angeles")
        ).strftime("%Y-%m-%d %H:%M:%S %Z")
        message_lines += [
            f"<https://cloud.samsara.com/o/{enrollment.org_id}/fleet/mdm|{enrollment.name}> - {enrollment.org_id} - {enrollment.id} - {created_at_str}"
        ]

    if len(enrollments) == 0:
        message_lines += ["No new enrollments during this period."]

    return message_lines


slack_channel = "mem-enrollment-notifications"
# Comment the next 4 lines and manually input dates into get_latest_enrollments to run it manually over a time period
start_date = get_date_x_days_ago(2)
end_date = get_date_x_days_ago(1)

start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")
# start_date_str = "2023-03-01"
# end_date_str = "2023-04-01"

message_lines = get_latest_enrollments(start_date_str, end_date_str)
client = get_client()
try:
    response = client.chat_postMessage(
        channel=slack_channel, text="\n".join(message_lines)
    )
except SlackApiError as e:
    print(e.response["error"])


# COMMAND ----------
