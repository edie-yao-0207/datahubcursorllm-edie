# Databricks notebook source
# MAGIC %run /backend/backend/recruiting_bot/recruiting_bot_metrics

# COMMAND ----------

# Atlanta - Account Development Representative - 916394
# Atlanta - Account Development Representative - French Fluency - 988621

jobs = [916394, 988621]
slack_channel = ["alerts-data-science"]
team_name = "Atlanta ADR"
role_name = "Atlanta ADR"
interview_comment = ""

recruiting_bot(
    slack_channel,
    team_name,
    role_name,
    jobs=jobs,
    interview_comment=interview_comment,
    include_candidate_name_in_open_offers_table=True,
)
