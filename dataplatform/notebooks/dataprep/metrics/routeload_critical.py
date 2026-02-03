# Databricks notebook source
# MAGIC %run /backend/backend/bigquery/bigquery_pipeline
# COMMAND ----------

bigquery_pipeline(
    "backend.routeloadcriticalcomponent",
    "Timestamp",
    "routeload_logs",
    "routeload_critical",
)
