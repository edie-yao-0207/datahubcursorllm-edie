# Databricks notebook source
# MAGIC %run /backend/backend/recruiting_bot/recruiting_bot_metrics

# COMMAND ----------

# San Jose - Account Development Representative - 612736
# San Francisco - Account Development Representative - 365191

jobs = [612736, 365191]
slack_channel = ["alerts-data-science"]
team_name = "Bay Area ADR"
role_name = "Bay Area ADR"
interview_comment = ""

recruiting_bot(
    slack_channel,
    team_name,
    role_name,
    jobs=jobs,
    interview_comment=interview_comment,
    include_candidate_name_in_open_offers_table=True,
)
