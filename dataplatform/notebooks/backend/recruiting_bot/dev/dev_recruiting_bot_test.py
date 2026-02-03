# Databricks notebook source
# MAGIC %run /backend/backend/recruiting_bot/recruiting_bot_metrics

# COMMAND ----------

# Data Science - 71002
# Hardware Engineering - 29125
# Software Engineering - 29126
# Product - 29122
# Product Design - 61041
# Quality Engineering - 62170

departments = [71002, 29125, 29126, 29122, 61041, 62170]
slack_channel = ["alerts-data-science"]
team_name = "dev"
role_name = "engineering and product"
interview_comment = "Python bug squash anyone?"

recruiting_bot(
    slack_channel,
    team_name,
    role_name,
    departments=departments,
    interview_comment=interview_comment,
)
