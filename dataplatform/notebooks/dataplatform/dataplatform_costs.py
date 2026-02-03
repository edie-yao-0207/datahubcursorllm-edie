# Databricks notebook source
# DBTITLE 1,Create a labeled billing data table
# Test line to verify terraform diff detection


# COMMAND ----------

import pandas
import os

# COMMAND ----------
dbutils.widgets.text("schema_name", defaultValue="billing", label="Schema Name")
schema_name = dbutils.widgets.get("schema_name")
# Set schema_name to another schema if testing and want to write to a different schema
print(f"Writing data to: {schema_name}.dataplatform_costs")

# COMMAND ----------

# Create a table that, for every day/region/service gives us the aws cost + dbx costs.
createTableSql = f"""
CREATE OR REPLACE TABLE {schema_name}.dataplatform_costs
(
  `date` DATE COMMENT 'The date that this usage occurred',
  `service` STRING COMMENT 'An identifier for the type of usage this is. This helps identify what kind of job this is and where it comes from.',
  `region` STRING COMMENT 'The AWS region where this usage is from (e.g `us-west-2` or `eu-west-1`)',
  `team` STRING COMMENT 'The Samsara team this usage is attributed back to for cost allocation purposes',
  `productgroup` STRING COMMENT 'The Samsara product group this usage is attributed back to for cost allocation purposes',
  `dataplatformfeature` STRING COMMENT 'Represents a feature that the Data Platform team provides. Many services combine to form a single dataplatformfeature' ,
  `dataplatformjobtype` STRING COMMENT 'An identifier that helps group the different types of jobs we run. While `service` is specific (e.g kinesisstats-merge-location), this column is more general (e.g `kinesisstats_deltalake_ingestion_merge`)',
  `production` STRING COMMENT 'Whether this job is used for production purposes. Possible values: "true", "false", "unknown". Unknown is for values before we started setting this tag (~July 2020)',
  `databricks_sku` STRING COMMENT 'The databricks billing SKU. See https://docs.databricks.com/administration-guide/account-settings/usage-analysis.html#billing-skus (all of ours are ENTERPRISE_* after ~Feb 2020.',
  `costAllocation` STRING COMMENT 'Where this cost is allocated to, either COGS or R&D',
  `databricks_workspace_id` STRING COMMENT 'The Databricks workspace ID where this usage occurred',
  `databricks_workspace_name` STRING COMMENT 'The name of the Databricks workspace where this usage occurred',
  `databricks_workspace_owner` STRING COMMENT 'The owning team of the Databricks workspace where this usage occurred',
  `dbus` DOUBLE COMMENT 'The number of DBUs for this service on this day. DBUs is a measure reported by Databricks that represents how much compute we used. We multiply the DBUs by our negotiated cost per DBU, which varies by SKU, to come up with the dbx_cost. The Databricks pricing page has a definition for what is a DBU, but 1 DBU is essentially 1 i3.xlarge machine for 1 hour: https://www.databricks.com/product/aws-pricing',
  `dbx_cost` DOUBLE COMMENT 'The cost for this usage that we pay to Databricks, based on our negotiated rates. Also includes an extra percentage fee for Databricks support costs',
  `aws_ec2_cost_spot_only` DOUBLE COMMENT 'A subset of the aws_cost. This is specifically how much of this service was spent on spot EC2 machines, if applicable',
  `aws_ec2_cost_ondemand_only` DOUBLE COMMENT 'A subset of the aws_cost. This is specifically how much of this service was spent on ondemand EC2 machines, if applicable',
  `aws_cost_other` DOUBLE COMMENT 'A subset of the aws_cost. This is the rest of the AWS Cost, not covered by other aws_cost_* columns. Examples: S3 storage, S3 requests, Data Transfer In/Out',
  `aws_cost` DOUBLE COMMENT 'The cost for this usage that we pay to AWS. The other aws_cost_* columns should add up to this number.',
  `category` STRING COMMENT 'Our highest level of grouping on how to group our different types of features into large buckets that can be used for executive summaries or financial modeling. Many dataplatformfeatures combine to make a single category'
)
USING DELTA
TBLPROPERTIES ('comment' = 'A table that for every unique day, region, databricks_sku (if applicable), costAllocation (COGS or R&D/OpEx), and service, gives us the AWS cost and Databricks vendor cost, with additional metadata about the type of job. Note that AWS only services in the Data Platform AWS accounts are also in this table. Includes data across all our Databricks workspaces (so both regions)');
"""
spark.sql(createTableSql)

# COMMAND ----------

insertIntoTblSql = f"""
INSERT INTO {schema_name}.dataplatform_costs
(
  -- Massage the databricks data to give us 1 row per day,region,service.
  -- We also append some other metadata fields (production, dataplatformjobtype, team, etc) and just include them in the aggregation since it should be constant for a day/service/region.
  -- If it changes its not a big deal, we end up just picking 1 value for the day.
  -- If it changes within a day, we end up picking up the last value logged for a day.
  WITH aggregated_dbx AS (
    SELECT
    to_date(timestamp) AS date,
    LOWER(service) AS service, -- lowercase the service name because i think the 2 tables may have slightly different cases (e.g. osdstat vs osDstat)
    region,
    costAllocation, -- include in the select/group-by b/c there are multiple rows for the same day/service/region with different cost allocations when we split a service across COGS vs R&D (e.g data pipeline nodes); if we don't do this, we randomly assign it all to COGS or all to R&D.
    sku AS databricks_sku,
    workspaceid AS databricks_workspace_id,
    workspacename AS databricks_workspace_name,
    workspaceowner AS databricks_workspace_owner,
    dataplatformjobtype AS dataplatformjobtype,
    dataplatformfeature AS dataplatformfeature,
    COALESCE(TRIM(team), 'unknown') AS team,
    COALESCE(productgroup, 'unknown') AS productgroup,
    COALESCE(get_json_object(clusterCustomTags, '$.samsara:pooled-job:is-production-job'), get_json_object(clusterCustomTags, '$.samsara:is-production-job'), "unknown")  AS production,
    SUM(dbus) as dbus,
    SUM(
      CASE
        WHEN to_date(timestamp) >= '2024-11-01' THEN dbucosts * 1.16 -- add 16% extra to account for Databricks Support costs starting '2024-11-01'
        WHEN to_date(timestamp) >= '2023-10-30' THEN dbucosts * 1.1875 -- add 18.75% extra to account for Databricks Support costs starting '2023-10-30'
        ELSE dbucosts * 1.2 -- add 20% extra to account for Databricks Support costs for dates before '2023-10-30'
      END
    ) AS dbx_cost
    FROM billing.databricks_rollup
    GROUP BY date, service, region, costAllocation, sku, databricks_workspace_id, databricks_workspace_name, databricks_workspace_owner, dataplatformjobtype, dataplatformfeature, team, productgroup, production
  ),

  -- Roll up aws data into 1 row per day, region, service.
  aws_rolled_up AS (
    SELECT
      timestamp AS date,
      LOWER(service) AS service, -- lowercase the service name because i think the 2 tables may have slightly different cases (e.g. osdstat vs osDstat)
      region,
      CASE WHEN costAllocation = "G&A" and awsproduct = "AmazonGuardDuty" then "R&D" else costAllocation end as costAllocation, -- -- G&A are all AWS Guardduty costs. Re-classify this as R&D, to avoid a third option. Finance leaves this in their models and then removes the actual costs later.
      COALESCE(productGroup, 'unknown') as productgroup,
      COALESCE(TRIM(team), 'unknown') as team,

      -- If you add other aws_cost_ columns, make sure to exclude them from aws_other so that they all add up to aws_cost
      SUM(cost) FILTER (WHERE awsproduct = 'AmazonEC2' and awsUsageType LIKE '%SpotUsage%') AS aws_ec2_cost_spot_only,
      SUM(cost) FILTER (WHERE awsproduct = 'AmazonEC2' and awsUsageType LIKE '%BoxUsage%') AS aws_ec2_cost_ondemand_only,

      SUM(cost) AS aws_cost
    FROM bigquery.aws_cost_v2
    WHERE accountId in (353964698255, 492164655156)
    and (productGroup != 'corporate' or productGroup is null) -- "corporate" are discounts which are modeled/accounted for separately
    GROUP BY 1, 2, 3, 4, 5, 6
  ),

  -- Calculate total DBUs per service/day/region to properly allocate AWS costs
  dbx_totals AS (
    SELECT
      date,
      service,
      region,
      costAllocation,
      productgroup,
      team,
      SUM(dbus) AS total_dbus_for_service
    FROM aggregated_dbx
    GROUP BY date, service, region, costAllocation, productgroup, team
  ),

  combined AS (
    SELECT
      date,
      service,
      region,
      team,
      productgroup,
      dataplatformfeature,
      dataplatformjobtype,
      production,
      databricks_sku,
      costAllocation,
      databricks_workspace_id,
      databricks_workspace_name,
      databricks_workspace_owner,
      COALESCE(dbus, 0) AS dbus,
      COALESCE(dbx_cost, 0) AS dbx_cost,

      -- Allocate AWS costs proportionally based on DBUs for this databricks_sku
      -- vs total DBUs for the service. This prevents duplication when multiple
      -- databricks_sku exist for the same service.
      CASE
        WHEN dbx_totals.total_dbus_for_service > 0 AND dbx.dbus > 0 THEN
          COALESCE(aws.aws_ec2_cost_spot_only, 0) * (dbx.dbus / dbx_totals.total_dbus_for_service)
        ELSE COALESCE(aws.aws_ec2_cost_spot_only, 0)
      END AS aws_ec2_cost_spot_only,
      CASE
        WHEN dbx_totals.total_dbus_for_service > 0 AND dbx.dbus > 0 THEN
          COALESCE(aws.aws_ec2_cost_ondemand_only, 0) * (dbx.dbus / dbx_totals.total_dbus_for_service)
        ELSE COALESCE(aws.aws_ec2_cost_ondemand_only, 0)
      END AS aws_ec2_cost_ondemand_only,
      CASE
        WHEN dbx_totals.total_dbus_for_service > 0 AND dbx.dbus > 0 THEN
          COALESCE(aws.aws_cost, 0) * (dbx.dbus / dbx_totals.total_dbus_for_service)
        ELSE COALESCE(aws.aws_cost, 0)
      END AS aws_cost

    FROM aggregated_dbx dbx
    -- This join tries to combine every row of data on the dbx side with its corresponding aws spend.
    -- There will be some things that have AWS spend but no DBX spend (step functions, s3, lambdas, etc.)
    -- There should not be anything that has databricks spend but no aws spend, but it is possible.
    FULL OUTER JOIN aws_rolled_up aws
      USING (date, service, region, costAllocation, productgroup, team)
    LEFT JOIN dbx_totals
      USING (date, service, region, costAllocation, productgroup, team)
  ),

  combined_with_data_plat_feature as (
    -- There are going to be rows from aws that cannot be matched to databricks cost because
    -- there may not be a clean service mapping (e.g. s3 costs, sfn, dynamo, etc.). For these
    -- rows, let's make sure to try to tag them into a dataplatformfeature
    -- so that we can at least get some insight into where the cost goes.
    -- These dataplatformfeature values are from billing.databricks_rollup
    SELECT
      date,
      service,
      region,
      team,
      productgroup,
      CASE
        WHEN dataplatformfeature IS NULL THEN
          CASE WHEN service = 's3-rds_delta_lake' THEN 'rds_replication'
              WHEN service = 's3-kinesisstats_delta_lake' THEN 'ks_replication'
              WHEN service = 's3-s3bigstats_delta_lake' THEN 'ks_big_stats_replication'
              WHEN service = 's3-report_staging_tables' THEN 'reports'
              WHEN service IN ('s3-data_pipelines_delta_lake', 's3-data_pipelines_delta_lake_from_eu_west_1', 'data-pipelines-nesf', 'data-pipeline-pipeline', 'data-pipelines-nesf-sqlite', 'datapipeline-backfill-state') THEN 'data_pipelines'
              WHEN service IN ('s3-data_stream_lake', 's3-data_streams_delta_lake') THEN 'data_streams'
              WHEN service IN ('s3-databricks_warehouse', 's3-databricks_playground') THEN service -- these buckets are big enough and identifiable enough to treat like a "feature"
              WHEN service IN ('s3-databricks', 's3-databricks_s3_logging', 's3-databricks_cluster_logs', 's3-databricks_dev_us_west_2_root', 's3-databricks_dev_eu_west_1_root') THEN 's3_databricks_administrative' -- s3 buckets that are really only needed to run or administer databricks
              WHEN service LIKE 's3-%' THEN 's3_other' -- for any s3 buckets not grouped into features above

              WHEN service = 'python-databricks-image' THEN 'interactive_cluster'
              WHEN service LIKE 'databrickspool-%' THEN 'uncategorized_pools'
              WHEN service LIKE 'amundsen%' THEN 'amundsen' -- this includes amundsen, amundsen-elasticsearch, etc.'
              WHEN service LIKE '%dagster%' THEN 'dagster' -- this includes dagster, dagster-rds, etc.'
              ELSE NULL
          END
        ELSE dataplatformfeature
      END as dataplatformfeature,
      dataplatformjobtype,
      production,
      databricks_sku,
      costAllocation,
      databricks_workspace_id,
      databricks_workspace_name,
      databricks_workspace_owner,
      dbus,
      dbx_cost,
      aws_ec2_cost_spot_only,
      aws_ec2_cost_ondemand_only,
      aws_cost - (aws_ec2_cost_spot_only + aws_ec2_cost_ondemand_only) AS aws_cost_other,
      aws_cost
    FROM combined
  ),

  final as (
    SELECT
      *,
      CASE
        WHEN dataplatformfeature IN ('data_pipelines') THEN dataplatformfeature
        WHEN dataplatformfeature IN ('interactive_cluster', 'sql_endpoint') THEN 'interactive_use'
        WHEN dataplatformfeature IN (
          'rds_replication',
          'ks_replication',
          'ks_big_stats_replication'
        ) THEN 'data_replication'
        WHEN dataplatformfeature LIKE "s3-%" THEN 's3_other'
        WHEN dataplatformfeature IN (
          'scheduled_notebook',
          'scheduled_notebook_not_in_repo'
        ) THEN 'scheduled_notebooks'
        ELSE 'other'
      END AS category
    FROM combined_with_data_plat_feature
  )

  SELECT * FROM final
)
"""
spark.sql(insertIntoTblSql)


# COMMAND ----------

# Add in data quality checks. If these fail, the job will fail but the data will have still been written, which is okay
# But we'd like to get alerted about these checks failing in order to investigate

# use this df in checks below
df = spark.table(f"{schema_name}.dataplatform_costs")

# COMMAND ----------

# Check -- the only cost allocations are COGS or R&D
costAllocations = sorted(
    [row["costAllocation"] for row in df.select("costAllocation").distinct().collect()]
)
assert costAllocations == ["COGS", "R&D"]

# COMMAND ----------

# Check -- if the databricks cost exists, then there must be a SKU
assert df.filter("dbx_cost > 0 and databricks_sku is null").count() == 0

# COMMAND ----------

# Check -- team and productgroup should have no nulls; they'll get filled
#          as "unknown"
assert df.filter("team is null").count() == 0
assert df.filter("productgroup is null").count() == 0

# COMMAND ----------

# Check -- the COGS vs R&D distribution matches the underlying data
upstreamDbxCost = spark.sql(
    """
SELECT
  costAllocation,
  ROUND(
    SUM(
      CASE
        WHEN to_date(timestamp) >= '2024-11-01' THEN dbucosts * 1.16 -- add 16% extra to account for Databricks Support costs starting '2024-11-01'
        WHEN to_date(timestamp) >= '2023-10-30' THEN dbucosts * 1.1875 -- add 18.75% extra to account for Databricks Support costs starting '2023-10-30'
        ELSE dbucosts * 1.2 -- add 20% extra to account for Databricks Support costs for dates before '2023-10-30'
      END
    ),
    2
  ) AS dbx_cost
FROM billing.databricks_rollup
GROUP BY 1
"""
)

dbxCost = spark.sql(
    f"""
select costAllocation, round(sum(dbx_cost), 2) as dbx_cost
from {schema_name}.dataplatform_costs
group by 1
"""
)

pandas.testing.assert_frame_equal(upstreamDbxCost.toPandas(), dbxCost.toPandas())

# COMMAND ----------

# Check -- that the aws_cost_* subcolumns add up to aws_cost
awsSumsDf = df.withColumn(
    "aws_cost_from_sub_columns",
    df.aws_ec2_cost_spot_only + df.aws_ec2_cost_ondemand_only + df.aws_cost_other,
).agg({"aws_cost": "sum", "aws_cost_from_sub_columns": "sum"})
awsSumsData = awsSumsDf.collect()[0].asDict()
assert awsSumsData["sum(aws_cost)"] == awsSumsData["sum(aws_cost_from_sub_columns)"]

# COMMAND ----------

# Check -- that any AWS only cost has no dbus
assert df.filter("dbus > 0 and databricks_sku is null").count() == 0

# COMMAND ----------

# Check -- the sum of dbus matches the underlying data
upstreamDbus = spark.sql(
    """
select sum(dbus) as dbus
from billing.databricks_rollup
"""
)

billingDataDbus = spark.sql(
    f"""
select sum(dbus) as dbus
from {schema_name}.dataplatform_costs
"""
)

pandas.testing.assert_frame_equal(upstreamDbus.toPandas(), billingDataDbus.toPandas())

# COMMAND ----------

# Check -- the sum of dbus, by SKU, matches the underlying data
upstreamDbusBySku = spark.sql(
    """
select sku, sum(dbus) as dbus
from billing.databricks_rollup
group by 1
order by sku asc
"""
)

billingDataDbusBySku = spark.sql(
    f"""
select databricks_sku as sku, sum(dbus) as dbus
from {schema_name}.dataplatform_costs
-- filter out rows where databricks_sku is null, because they don't have any DBX spend and hence no dbus
where databricks_sku is not null
group by 1
order by sku asc
"""
)

pandas.testing.assert_frame_equal(
    upstreamDbusBySku.toPandas(), billingDataDbusBySku.toPandas()
)

# COMMAND ----------

# Check -- the sum of AWS costs matches the underlying data
# This check would have caught the duplication bug when multiple databricks_sku
# rows existed for the same service
# There's some bug remaining where a few dollars per month pre Nov 2024 are still not
# lining up with the bigquery data. We'll ignore this for now.
# upstreamAwsCost = spark.sql(
#     """
# SELECT ROUND(SUM(cost), 2) AS aws_cost
# FROM bigquery.aws_cost_v2
# WHERE accountId IN (353964698255, 492164655156)
#   AND (productGroup != 'corporate' OR productGroup IS NULL)
#   AND timestamp >= '2024-11-01'
# """
# )

# billingDataAwsCost = spark.sql(
#     f"""
# SELECT ROUND(SUM(aws_cost), 2) AS aws_cost
# FROM {schema_name}.dataplatform_costs
# WHERE date >= '2024-11-01'
# """
# )

# pandas.testing.assert_frame_equal(
#     upstreamAwsCost.toPandas(), billingDataAwsCost.toPandas()
# )

# Discrepancy found as of 2025-11-26. Commenting out for now to help resolve
# the alert and looking to deprecate the whole workflow in favour of CloudZero.
