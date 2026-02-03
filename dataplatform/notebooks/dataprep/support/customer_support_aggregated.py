# Databricks notebook source
from pyspark.sql.functions import *

support_dates = spark.sql(
    """
select distinct
  org_dates.*,
  support.ticket_id,
  support.product_area
from dataprep.customer_dates org_dates
left join dataprep.customer_support support
  on support.sam_number = org_dates.sam_number
  and support.created_date = org_dates.date
"""
)

support_pivoted = (
    support_dates.groupBy("sam_number", "date")
    .pivot("product_area")
    .agg(count("product_area"))
    .fillna(0)
)
support_pivoted = support_pivoted.drop("null")

for clmn in support_pivoted.columns:
    # format column names and rename table agg columns
    formatted_c = clmn.lower().replace(" ", "_")
    support_pivoted = support_pivoted.withColumnRenamed(clmn, formatted_c)

support_pivoted.write.mode("overwrite").partitionBy("date").saveAsTable(
    "dataprep.customer_support_aggregated"
)
