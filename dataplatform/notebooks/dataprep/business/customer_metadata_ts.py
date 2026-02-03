# Databricks notebook source
from pyspark.sql.functions import *

# group by all rows except org ID to keep data from customer_metadata table but group by SAM number to combine all orgs that belong to the same account
salesforce_accounts = spark.table("dataprep.customer_metadata")

agg_expr = {
    x: "first" for x in salesforce_accounts.columns if x not in {"org_id", "sam_number"}
}
agg_expr["org_id"] = "collect_set"
salesforce_accounts_agg = salesforce_accounts.groupBy("sam_number").agg(agg_expr)

# rename resulting aggregate columns
for c in salesforce_accounts.columns:
    salesforce_accounts_agg = salesforce_accounts_agg.withColumnRenamed(
        "first({})".format(c), c
    )
salesforce_accounts_agg = salesforce_accounts_agg.withColumnRenamed(
    "collect_set(org_id)", "org_ids"
)

salesforce_accounts_agg = salesforce_accounts_agg.drop("csm_rating")
salesforce_accounts_agg.createOrReplaceTempView("salesforce_customer_metadata")

# COMMAND ----------

# duplicates coming from metadata tracker as a result of that table having multiple org ids per sam number
metadata_rating_ts = spark.sql(
    """
  select distinct
    metadata.*,
    org_dates.date,
    metadata_dates.csm_rating
  from dataprep.customer_dates org_dates
  join salesforce_customer_metadata metadata
    on metadata.sam_number = org_dates.sam_number
  left join dataprep.customer_health_tracking metadata_dates
    on org_dates.sam_number = metadata_dates.sam_number
    and org_dates.date = metadata_dates.date
"""
)

# COMMAND ----------

metadata_rating_ts.write.format("delta").mode("ignore").partitionBy("date").saveAsTable(
    "dataprep.customer_metadata_ts"
)
metadata_rating_updates = metadata_rating_ts.filter(
    col("date") >= date_sub(current_date(), 3)
)
metadata_rating_updates.write.format("delta").mode("overwrite").partitionBy(
    "date"
).option("replaceWhere", "date >= date_sub(current_date(), 3)").saveAsTable(
    "dataprep.customer_metadata_ts"
)
