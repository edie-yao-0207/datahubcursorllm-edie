# Databricks notebook source
# MAGIC %run /backend/backend/bigquery/bigquery_pipeline

# COMMAND ----------

bigquery_pipeline(
    "backend.livestream_webrtc_browser_metrics",
    "ServerTime",
    "connectedworker",
    "livestream_webrtc_browser_metrics",
)
