# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Org Segments

# COMMAND ----------

# MAGIC %run /backend/dataproducts/benchmarking/org_segmentation/org_segments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Save previous org segments to enable comparison stats
# MAGIC drop table if exists dataproducts.org_segments_v2_prev;
# MAGIC create table if not exists dataproducts.org_segments_v2_prev as (
# MAGIC   select * from dataproducts.org_segments_v2
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dataproducts.org_segments_v2;
# MAGIC create table if not exists dataproducts.org_segments_v2 as (
# MAGIC   select * from playground.org_segments_v2
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Debugging touchpoint
# MAGIC select * from dataproducts.org_segments_v2
# MAGIC where org_id = 562949953423927;

# COMMAND ----------

s3_bucket = f"s3://samsara-benchmarking-metrics"
query = "select * from dataproducts.org_segments_v2"

org_segments_sdf = spark.sql(query)
org_segments_df = org_segments_sdf.toPandas()
org_segments_df.head()

org_segments_sdf.coalesce(1).write.format("csv").mode("overwrite").option(
    "header", True
).save(f"{s3_bucket}/org_segments_v2")
