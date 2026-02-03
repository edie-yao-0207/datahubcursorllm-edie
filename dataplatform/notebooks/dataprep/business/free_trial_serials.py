# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *

spark.read.format("bigquery").option(
    "table", "netsuite_devices.free_trial_serials"
).load().createOrReplaceTempView("free_trial_serials")

free_trial_serials = spark.sql(
    """
select
    *
from free_trial_serials
"""
)

free_trial_serials.write.format("delta").mode("overwrite").saveAsTable(
    "dataprep.free_trial_serials"
)
