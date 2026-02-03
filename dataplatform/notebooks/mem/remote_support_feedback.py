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
      organizations.name,
      feedback_type,
      custom_feedback,
      channel_uuid
    from datastreams.remote_support_feedback_log
    join clouddb.organizations on org_id = organizations.id
    where remote_support_feedback_log.timestamp >= "{start_date}" and timestamp < "{end_date}" and feedback_type != "none_chosen" and organizations.internal_type != 1
    order by timestamp
  """

    feedback_entries = spark.sql(query).rdd.collect()

    message_lines = []
    if len(feedback_entries) == 0:
        message_lines += [
            f"*No new feedback entries between {start_date} and {end_date}.*"
        ]
    else:
        message_lines += [
            f"*:bell: {len(feedback_entries)} new feedback entries between {start_date} and {end_date}:*"
        ]

        for feedback in feedback_entries:
            feedback_reason = feedback.feedback_type.replace("_", " ")
            message = f"{feedback.name} ({feedback.org_id}) session failed. Reason: {feedback_reason}. Channel UUID: {feedback.channel_uuid}"
            if feedback.custom_feedback != "":
                message += f'Custom feedback: "_{feedback.custom_feedback}_"'

            message_lines += [message]

    return message_lines


slack_channel = "mem-remote-support-feedback-notifs"
start_date = get_date_x_days_ago(2)
end_date = get_date_x_days_ago(1)

start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

# Replace start and end data with these to query a date range
# start_date_str = "2024-07-11"
# end_date_str = "2024-07-15"

message_lines = get_latest_enrollments(start_date_str, end_date_str)
client = get_client()
try:
    response = client.chat_postMessage(
        channel=slack_channel, text="\n".join(message_lines)
    )
except SlackApiError as e:
    print(e.response["error"])

# COMMAND ----------
