# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace temporary view cellular_location_vodafone_test as (
# MAGIC   select
# MAGIC     vodafone.product_id as vodafone_product_id,
# MAGIC     vodafone.hex_id_res_8 as vodafone_hex_id_res_8,
# MAGIC     vodafone.hex_lat as vodafone_hex_id_res_8_lat,
# MAGIC     vodafone.hex_lon as vodafone_hex_id_res_8_lon,
# MAGIC     vodafone.avg_hex_delay as vodafone_avg_hex_delay,
# MAGIC     vodafone.median_device_avg_hex_delay as vodafone_median_device_avg_hex_delay,
# MAGIC     vodafone.avg_device_median_hex_delay as vodafone_avg_device_median_hex_delay,
# MAGIC     vodafone.median_device_median_hex_delay as vodafone_median_device_median_hex_delay,
# MAGIC     vodafone.avg_device_avg_hex_rssi_dbm as vodafone_avg_device_avg_hex_rssi_dbm,
# MAGIC     vodafone.median_device_avg_hex_rssi_dbm as vodafone_median_device_avg_hex_rssi_dbm,
# MAGIC     vodafone.avg_device_median_hex_rssi_dbm as vodafone_avg_device_median_hex_rssi_dbm,
# MAGIC     vodafone.median_device_median_hex_rssi_dbm as vodafone_median_device_median_hex_rssi_dbm
# MAGIC    from playground.vodafone_h3_index_metric_hex_id_res_8_v2 as vodafone
# MAGIC );

# COMMAND ----------

df = spark.table("cellular_location_vodafone_test")

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

df.head()

df.describe()

# COMMAND ----------
