# Databricks notebook source
# MAGIC %run /backend/backend/bigquery/bigquery_pipeline

# COMMAND ----------

bigquery_pipeline(
    "samsara-data.backend_test.aws_cost_drivers", "date", "bigquery", "aws_cost_drivers"
)
