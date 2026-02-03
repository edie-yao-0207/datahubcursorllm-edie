-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Update device_type in dim_devices and dim_devices_fast
-- MAGIC
-- MAGIC Set `device_type` to Asset Tracker for AT11 devices bsed on the product ID (172 = AT11 based on the definitions.products table)

-- COMMAND ----------

update
  datamodel_core.dim_devices
set
  device_type = 'AT - Asset Tracker'
where
  product_id = 172
  and date between coalesce(nullif(getArgument("start_date"), ''), '2024-06-01') and coalesce(nullif(getArgument("end_date"), ''), '2024-11-10')

-- COMMAND ----------

update
  datamodel_core.dim_devices_fast
set
  device_type = 'AT - Asset Tracker'
where
  product_id = 172
  and date between coalesce(nullif(getArgument("start_date"), ''), '2024-06-01') and coalesce(nullif(getArgument("end_date"), ''), '2024-11-10')
