# Databricks notebook source
import requests
import re
import pandas as pd
from delta.tables import *
from pyspark.sql.functions import current_timestamp, split, explode, trim, col, lit

# Get jiras from zendesk tickets linked to warranty_exchanges

tickets = spark.sql(
    f"""select distinct serial, custom_jira_key as jira from hardware.hardware_exchanges_sf where custom_jira_key  is not null and custom_jira_key  != ''"""
)

# Split the 'jira' column on comma and split into separate rows in case of multiples
df_split = tickets.withColumn("jira_split", split(col("jira"), ","))
df_exploded = df_split.withColumn("jira_exploded", explode(col("jira_split")))
df_trimmed = df_exploded.withColumn("jira_trimmed", trim(col("jira_exploded")))
df_result = df_trimmed.select("serial", "jira_trimmed").withColumnRenamed(
    "jira_trimmed", "jira"
)
zendesk_df = df_result.withColumn("added_by", lit("automated_from_zendesk_ticket"))
zendesk_df = zendesk_df.withColumn("date_added", current_timestamp())
# Update table only with new entries
existing_table = DeltaTable.forName(spark, "hardware_analytics.sn_to_jira")
existing_table.alias("original").merge(
    zendesk_df.alias("updates"),
    "original.serial = updates.serial and original.jira = updates.jira",
).whenNotMatchedInsertAll().execute()

# COMMAND ----------
