# Databricks notebook source
# MAGIC %run /backend/backend/recruiting_bot/recruiting_bot_metrics

# COMMAND ----------

# Accounting - 78370
# Corporate Accounting - 78383
# Finance Ops - 78387
# Payroll and Equity - 78384
# Tax - 78386
# Procurement - 78385
# Treasury - 78388

departments = [78370, 78383, 78387, 78384, 78386, 78385, 78388]
slack_channel = ["finance_directors"]
team_name = "Finance"
role_name = "finance"
interview_comment = ""

recruiting_bot(
    slack_channel,
    team_name,
    role_name,
    departments=departments,
    interview_comment=interview_comment,
)
