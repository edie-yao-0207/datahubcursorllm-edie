# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")

workspace_name = str.split(spark.conf.get("spark.databricks.workspaceUrl"), ".")[0]
region_name = str.replace(workspace_name, "samsara-dev-", "")

if region_name == "eu-west-1":
    catalog_name = "edw_delta_share"
else:
    catalog_name = "edw"

devices_sold = spark.sql(
    f"""
select
    date(t.trandate) as transaction_date,
    replace(i_num.serial, '-', '') as serial,
    replace(i.item_name, 'HW-', '') as product,
    t.transaction_type,
    substring(c.salesforce_id_2, 1, 15) as sfdc_id,
    replace(c.sam_number, '-', '') as sam_number,
    cf.order_type
from {catalog_name}.netsuite_sterling.transaction_inventory_numbers i_num
left join {catalog_name}.netsuite_sterling.transactions t
on i_num.transaction_id = t.transaction_id
left join {catalog_name}.silver.fct_orders tl
on i_num.transaction_line_id = tl.transaction_line_id and i_num.transaction_id = tl.transaction_id
left join {catalog_name}.netsuite_sterling.item_product_group i
on i.item_id = tl.item_id
left join {catalog_name}.netsuite_sterling.netsuite_customer c
on c.customer_id = case when t.transaction_type like '%Receipt%' then t.entity_id else t.end_customer_id end
left join {catalog_name}.netsuite_sterling.transactions cf
on t.created_from_id = cf.transaction_id
where t.transaction_type in ('Item Fulfillment', 'Item Receipt')
and i.item_name LIKE '%HW%'
and cf.transaction_type in ('Sales Order', 'Return Authorization')
and t.is_deleted = false
"""
)

licenses = spark.sql(
    f"""
select
  date(t.trandate) as transaction_date,
  null as serial,
  i.item_name as product,
  t.transaction_type,
  substring(c.salesforce_id_2, 1, 15) as sfdc_id,
  replace(c.sam_number, '-', '') as sam_number,
  t.order_type
from {catalog_name}.netsuite_sterling.transactions t
left join {catalog_name}.silver.fct_orders tl
on t.transaction_id = tl.transaction_id
left join {catalog_name}.netsuite_sterling.item_product_group i
on i.item_id = tl.item_id
left join {catalog_name}.netsuite_sterling.netsuite_customer c
on c.customer_id = case when t.transaction_type like '%Return%' then t.entity_id else t.end_customer_id end
left join {catalog_name}.netsuite_sterling.transactions cf
on t.created_from_id = cf.transaction_id
where t.transaction_type in ('Sales Order', 'Return Authorization')
and i.item_name LIKE '%LIC%'
and t.is_deleted = false
"""
)

sam_org_list = spark.sql(
    """
select
    sf.sam_number,
    collect_list(osf.org_id) as org_id_list
from clouddb.org_sfdc_accounts osf
left join clouddb.sfdc_accounts sf on
    osf.sfdc_account_id = sf.id
group by sam_number
"""
)

devices_sold = devices_sold.union(licenses)

devices_sold = devices_sold.join(sam_org_list, ["sam_number"])

devices_sold.write.format("delta").mode("overwrite").option(
    "mergeSchema", "true"
).saveAsTable("dataprep.devices_sold")
