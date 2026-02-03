# Databricks notebook source
# MAGIC %run /backend/backend/bigquery/bigquery_pipeline

# COMMAND ----------

bigquery_pipeline(
    "backend.workforcevideostreamstart",
    "ServerStartTimestamp",
    "connectedworker",
    "workforcevideostreamstart",
)
