# Databricks notebook source
# MAGIC %run /backend/backend/recruiting_bot/recruiting_bot_metrics

# COMMAND ----------

# Support - 29121

departments = [29121]
slack_channel = ["support-leadership", "support-ops-team"]
team_name = "Technical Support"
role_name = "support"
interview_comment = ""

recruiting_bot(
    slack_channel,
    team_name,
    role_name,
    departments=departments,
    interview_comment=interview_comment,
    email_flag=False,
    employment_type=True,
    include_all_accepted_offers=True,
)
