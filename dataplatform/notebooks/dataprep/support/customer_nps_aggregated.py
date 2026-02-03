# Databricks notebook source
from pyspark.sql.functions import *

org_dates = spark.table("dataprep.customer_dates")
cnps_data = spark.sql(
    """
                      select
                        date,
                        raw_score,
                        nps_score,
                        sam_number
                      from dataprep.customer_nps
                      """
)

cnps_data = (
    org_dates.join(cnps_data, ["sam_number", "date"], "left")
    .groupBy("sam_number", "date")
    .agg(
        avg("raw_score").alias("avg_raw_score"), avg("nps_score").alias("avg_nps_score")
    )
)

cnps_data.write.format("delta").mode("ignore").partitionBy("date").saveAsTable(
    "dataprep.customer_nps_aggregated"
)
cnps_data_updates = cnps_data.filter(col("date") >= date_sub(current_date(), 3))
cnps_data_updates.write.format("delta").mode("overwrite").partitionBy("date").option(
    "replaceWhere", "date >= date_sub(current_date(), 3)"
).saveAsTable("dataprep.customer_nps_aggregated")
