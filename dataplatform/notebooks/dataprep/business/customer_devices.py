# Databricks notebook source
from pyspark.sql.functions import *

spark.read.format("bigquery").option(
    "table", "backend.org_sfdc_account_latest"
).load().createOrReplaceTempView("sam_orgs")

# COMMAND ----------

org_metadata = spark.sql(
    """
select 
  id,
  sam_orgs.SamNumber as sam_number
from clouddb.organizations orgs
join sam_orgs on id = sam_orgs.OrgId
where internal_type != 1
"""
)

org_metadata.createOrReplaceTempView("org_metadata")

# COMMAND ----------

devices_sold_agg = spark.sql(
    """
select 
  sam_number, 
  product,
  COUNT(product) as number_shipped
from dataprep.devices_sold devices_sold
join definitions.products on name = product
where transaction_type != 'Item Receipt'
group by sam_number, product
"""
)

devices_sold_agg.createOrReplaceTempView("devices_aggregated")

# COMMAND ----------

active_devices = spark.sql(
    """
select 
  metadata.sam_number,
  product_mappings.name as product_name
from org_metadata metadata
left join dataprep.devices_sold ds
  on metadata.sam_number = ds.sam_number
left join dataprep.device_heartbeats_extended hb
  on metadata.id = hb.org_id
  and ds.serial = hb.serial
  and last_heartbeat_date > transaction_date
  and hb.product_id is not null
join definitions.products product_mappings
  on product_mappings.product_id = hb.product_id
"""
)

active_devices.createOrReplaceTempView("active_devices")

# COMMAND ----------

active_devices_agg = spark.sql(
    """
select
  sam_number,
  product_name,
  count(product_name) as total_active_devices
from active_devices
group by
  sam_number,
  product_name
"""
)

active_devices_agg.createOrReplaceTempView("active_devices_agg")

# COMMAND ----------

ds_ad_joined = spark.sql(
    """
select 
  ds.sam_number,
  ds.product,
  ds.number_shipped,
  ad.total_active_devices as number_active
from devices_aggregated ds
left join active_devices_agg ad
  on ds.sam_number = ad.sam_number
  and ds.product = ad.product_name
"""
)

# COMMAND ----------

product_mappings = spark.table("definitions.products")
devices_pivoted = (
    ds_ad_joined.groupBy("sam_number")
    .pivot("product")
    .agg(first("number_shipped"), first("number_active"))
    .fillna(0)
)

for c in product_mappings.select(collect_list("name")).first()[0]:
    devices_pivoted = devices_pivoted.withColumnRenamed(
        "{}_first(number_shipped, false)".format(c), c
    )
    devices_pivoted = devices_pivoted.withColumnRenamed(
        "{}_first(number_active, false)".format(c), "{}_active".format(c)
    )

# COMMAND ----------

devices_pivoted.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "dataprep.customer_devices"
)
