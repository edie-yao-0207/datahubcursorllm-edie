# Databricks notebook source
# MAGIC %run /backend/backend/bigquery/bigquery_pipeline

# COMMAND ----------

bigquery_pipeline("backend.api", "Date", "api_logs", "api")
