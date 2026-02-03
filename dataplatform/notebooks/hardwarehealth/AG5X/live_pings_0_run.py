# Databricks notebook source
from datetime import date, datetime, time, timedelta

metrics_date = date.today() - timedelta(days=1)
try:
    dbutils.notebook.run("live_pings_1_build_trip_table", 0, {"date": metrics_date})
    dbutils.notebook.run("live_pings_2_build_metrics_table", 0, {"date": metrics_date})
except:
    None
