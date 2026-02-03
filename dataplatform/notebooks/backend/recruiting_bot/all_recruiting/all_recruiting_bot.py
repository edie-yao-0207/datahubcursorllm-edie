# Databricks notebook source
# MAGIC %run /backend/backend/recruiting_bot/recruiting_bot_metrics

# COMMAND ----------

# Big Data and Machine Learning - 71002
# Technical Support - 29121
# Workplace - 67041
# IT - 66670
# Business Systems - 65756
# Quality Engineering - 62170
# Customer Success - 62169
# Recruiting - 62168
# Product Design - 61041
# Finance - 57383
# Legal - 57193
# Business Operations - 56978
# Product - 29122
# Sales Operations - 51520
# Sales Engineering - 50003
# Marketing - 29124
# Software Engineering - 29126
# People Operations - 37421
# Hardware Engineering - 29125
# Operations - 29123
# Sales - 29120

departments = [
    71002,
    29121,
    67041,
    66670,
    65756,
    62170,
    62169,
    62168,
    61041,
    57383,
    57193,
    56978,
    29122,
    51520,
    50003,
    29124,
    29126,
    37421,
    29125,
    29123,
    29120,
]
slack_channel = ["recruiting_all"]
team_name = "Samsara"
role_name = "open"
interview_comment = ""

recruiting_bot(
    slack_channel,
    team_name,
    role_name,
    departments=departments,
    interview_comment=interview_comment,
    email_flag=False,
    include_open_offer_table=False,
    include_accepted_offer_table=False,
    allow_closing_candidates_thread=False,
)
