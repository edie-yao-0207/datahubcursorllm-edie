# Databricks notebook source
from pyspark.sql.functions import *

metadata = spark.table("dataprep.customer_metadata_ts")
ops = spark.table("dataprep.customer_operations")
support = spark.table("dataprep.customer_support_aggregated")
shipped_devices = spark.table("dataprep.customer_devices")
routeloads = spark.table("dataprep.customer_routeloads")
nps = spark.table("dataprep.customer_nps_aggregated")

# COMMAND ----------

shipped_join = metadata.join(shipped_devices, ["sam_number"], "inner")
ops_joined = ops.join(shipped_join, ["sam_number", "date"], "left")
routes_joined = ops_joined.join(routeloads, ["sam_number", "date"], "left")
support_joined = routes_joined.join(support, ["sam_number", "date"], "left")
consolidated = support_joined.join(nps, ["sam_number", "date"], "left")

# COMMAND ----------

consolidated.write.format("delta").mode("ignore").partitionBy("date").saveAsTable(
    "dataprep.customer_consolidated"
)
consolidated_updates = consolidated.filter(col("date") >= date_sub(current_date(), 3))
consolidated_updates.write.format("delta").mode("overwrite").partitionBy("date").option(
    "replaceWhere", "date >= date_sub(current_date(), 3)"
).saveAsTable("dataprep.customer_consolidated")
