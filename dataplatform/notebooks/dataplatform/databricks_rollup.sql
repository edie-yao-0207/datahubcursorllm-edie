-- Databricks notebook source
-- DBTITLE 1,Create a labeled billing data table
CREATE OR REPLACE TABLE billing.databricks_rollup
(
  `workspaceid` BIGINT COMMENT 'The databricks workspace ID that this usage comes from.',
  `workspacename` STRING COMMENT 'The name we have for the Databricks workspace ID.',
  `workspaceowner` STRING COMMENT 'The owning team of the Databricks workspace.',
  `region` STRING COMMENT 'The region that this workspace and usage was run in.',
  `timestamp` TIMESTAMP COMMENT 'The The end of the hour for the provided usage (e.g if usage ends at 10:30, this value would say 10:59)',
  `clusterid` STRING COMMENT 'The unique ID of the databricks cluster (automated or interactive) for this usage.',
  `clusternodetype` STRING COMMENT 'Instance type of the cluster (e.g i3.xlarge). If the usage uses a hybrid pool or a fleet pool where the driver and worker nodes use different instance types, then there will be multiple rows in this table for each instance type used.',
  `sku` STRING COMMENT 'The databricks billing SKU. See https://docs.databricks.com/administration-guide/account-settings/usage-analysis.html#billing-skus (all of ours are ENTERPRISE_* after ~Feb 2020.',
  `totaldbusnotsplit` DOUBLE COMMENT 'The total number of DBUs for this usage. If this usage is split by COGS and R&D, this number represents the total combined DBUs for COGS and R&D combined.',
  `totalmachinehoursnotsplit` DOUBLE COMMENT 'The total number of machine hours for this usage. If this usage is split by COGS and R&D, this number represents the total combined machine hours for COGS and R&D combined.',
  `team` STRING COMMENT 'The Samsara team who this usage is attributed back to for cost allocation purposes.',
  `productgroup` STRING COMMENT 'The Samsara product group who this usage is attributed back to for cost allocation purposes.',
  `service` STRING COMMENT 'An identifier for the type of usage this is. This helps identify what kind of job this is and where it comes from.',
  `clustercustomtags` STRING COMMENT 'A series of tags we set on jobs and clusters to identify and categorize them.',
  `dataplatformjobtype` STRING COMMENT 'An identifier that helps group the different types of jobs we run. While service is specific (e.g kinesisstats-merge-location), this column is more general (e.g kinesisstats_deltalake_ingestion_merge).',
  `dataplatformfeature` STRING COMMENT 'Our highest level of grouping on how to group our different types of usage.',
  `poolname` STRING COMMENT 'If the usage utilizes an instance pool, this is the name of the pool.',
  `iscostallocationsplit` BOOLEAN COMMENT 'Lets us know if this given usage is split into COGS vs R&D partially.',
  `dbus` DOUBLE COMMENT 'The amount of DBUs (databricks units) used for this usage during this hour.',
  `machinehours` DOUBLE COMMENT 'Total number of machine hours used by all containers in the cluster.',
  `costallocation` STRING COMMENT 'Either COGS or R&D.',
  `dbucosts` DOUBLE COMMENT 'The cost for this usage that we pay to Databricks, based on our negotiated rates. Note this does NOT include AWS cost, just the Databricks vendor cost.'
)
USING DELTA
TBLPROPERTIES ('comment' = 'A rollup of our databricks bill/usage where tags are extracted into columns to indicate information such as teams, product groups, etc. If a given piece of usage is split amongst COGS and R&D, there will be two rows in this view for that usage. There is another table that aggregates and joins this data into daily rows with AWS cost data: billing.dataplatform_costs.');

INSERT INTO billing.databricks_rollup (
WITH prices AS (
  -- STANDARD prefix is the old prefix databricks used (should no longer show up)
  -- PREMIUM prefix is used when the compute is not E2
  -- ENTERPRISE prefix is used when the compute is E2

  -- We should only use current_date() as an end_date when it is a SKU we no longer use (just in case it shows up for some reason)
  -- Otherwise, set an end_date to where we know the contract with that price ends (so we're forced to update this when contracts renew)

  -- Pricing negotiated started April 2020, then negotiated again in Nov 2020
  -- New 2 year contract started in Nov 2021
  -- New 1 year contract started in Nov 2023
  -- New 3 year contract started in Nov 2024
  VALUES
    -- ** Interactive / All Purpose Compute **
    (
      'STANDARD_INTERACTIVE_OPSEC',
      date('2019-01-01'),
      date('2020-04-01'),
      0.55
    ),
    (
      'STANDARD_INTERACTIVE_OPSEC',
      date('2020-04-01'),
      current_date(),
      0.395
    ),
    -- {PREMIUM|ENTERPRISE}_ALL_PURPOSE_COMPUTE is a databricks job that uses an interactive cluster. In other words, an interactive Notebook or job through JDBC connector.
    (
      'PREMIUM_ALL_PURPOSE_COMPUTE',
      date('2019-01-01'),
      date('2020-04-01'),
      0.55
    ),
    (
      'PREMIUM_ALL_PURPOSE_COMPUTE',
      date('2020-04-01'),
      date('2020-11-01'),
      0.395
    ),
    (
      'PREMIUM_ALL_PURPOSE_COMPUTE',
      date('2020-11-01'),
      current_date(),
      0.33
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_COMPUTE',
      date('2019-01-01'),
      date('2020-04-01'),
      0.55
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_COMPUTE',
      date('2020-04-01'),
      date('2020-11-01'),
      0.395
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_COMPUTE',
      date('2020-11-01'),
      date('2021-11-01'),
      0.33
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_COMPUTE',
      date('2021-11-01'),
      date('2023-11-01'),
      0.31
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_COMPUTE',
      date('2023-11-01'),
      date('2024-11-01'),
      0.31
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_COMPUTE',
      date('2024-11-01'),
      date('2027-10-29'),
      0.31 -- $0.65/dbu and we get a 52.31% discount.
    ),
    -- Note: I don't have the data for 2021-2023 for photon,
    -- so i'm reusing the value for the 2023-2024 contract.
    -- Given how old the data is it should be fine.
    (
      'ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)',
      date('2021-11-01'),
      date('2023-11-01'),
      0.5525
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)',
      date('2023-11-01'),
      date('2024-11-01'),
      0.5525
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_COMPUTE_(PHOTON)',
      date('2024-11-01'),
      date('2027-10-29'),
      0.31 -- $0.65/dbu and we get a 52.31% discount.
    ),

    -- ** Automated / Jobs Compute **
    (
      'STANDARD_AUTOMATED_OPSEC',
      date('2019-01-01'),
      date('2020-04-01'),
      0.2
    ),
    (
      'STANDARD_AUTOMATED_OPSEC',
      date('2020-04-01'),
      current_date(),
      0.11
    ),
    -- {PREMIUM|ENTERPRISE}_JOBS_COMPUTE is a databricks job that does not use an interactive cluster. In other words, a scheduled job or a job without an interactive Notebook UI.
    (
      'PREMIUM_JOBS_COMPUTE',
      date('2019-01-01'),
      date('2020-04-01'),
      0.2
    ),
    (
      'PREMIUM_JOBS_COMPUTE',
      date('2020-04-01'),
      date('2020-11-01'),
      0.11
    ),
    (
      'PREMIUM_JOBS_COMPUTE',
      date('2020-11-01'),
      current_date(),
      0.09
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE',
      date('2019-01-01'),
      date('2020-04-01'),
      0.2
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE',
      date('2020-04-01'),
      date('2020-11-01'),
      0.11
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE',
      date('2020-11-01'),
      date('2021-11-01'),
      0.09
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE',
      date('2021-11-01'),
      date('2023-11-01'),
      0.08
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE',
      date('2023-11-01'),
      date('2024-11-01'),
      0.08
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE',
      date('2024-11-01'),
      date('2027-10-29'),
      0.1 -- $0.2/dbu and we get a 50% discount.
    ),
    -- Note: I don't have the data for 2020-2023 for photon,
    -- so i'm reusing the value for the 2023-2024 contract.
    -- Given how old the data is at time of writing, it feels okay.
    (
      'ENTERPRISE_JOBS_COMPUTE_(PHOTON)',
      date('2020-04-01'),
      date('2020-11-01'),
      0.17
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE_(PHOTON)',
      date('2020-11-01'),
      date('2021-11-01'),
      0.17
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE_(PHOTON)',
      date('2021-11-01'),
      date('2023-11-01'),
      0.17
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE_(PHOTON)',
      date('2023-11-01'),
      date('2024-11-01'),
      0.17
    ),
    (
      'ENTERPRISE_JOBS_COMPUTE_(PHOTON)',
      date('2024-11-01'),
      date('2027-10-29'),
      0.1 -- $0.2/dbu and we get a 50% discount.
    ),

    -- ** SQL / "Databricks SQL" compute **
    -- {PREMIUM|ENTERPRISE}_SQL_COMPUTE is "Databricks SQL" (aka SQL Analytics aka Redash) compute
    (
      'PREMIUM_SQL_COMPUTE',
      date('2019-01-01'),
      date('2021-06-01'),
      0.15
    ),
    (
      'PREMIUM_SQL_COMPUTE',
      date('2021-06-01'),
      current_date(),
      0.22
    ),
    (
      'ENTERPRISE_SQL_COMPUTE',
      date('2019-01-01'),
      date('2021-06-01'),
      0.15
    ),
    (
      'ENTERPRISE_SQL_COMPUTE',
      date('2021-06-01'),
      date('2021-11-01'),
      0.22
    ),
    (
      'ENTERPRISE_SQL_COMPUTE',
      date('2021-11-01'),
      date('2023-11-01'),
      0.176
    ),
    (
      'ENTERPRISE_SQL_COMPUTE',
      date('2023-11-01'),
      date('2024-11-01'),
      0.176
    ),
    (
      'ENTERPRISE_SQL_COMPUTE',
      date('2024-11-01'),
      date('2027-10-29'),
      0.1496 -- $0.22/dbu and we get a 32% discount.
    ),
    (
      'ENTERPRISE_SQL_PRO_COMPUTE_US_WEST_OREGON',
      date('2023-11-01'),
      date('2024-11-01'),
      0.44
    ),
    (
      'ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_WEST_OREGON',
      date('2021-11-01'),
      date('2023-11-01'),
      0.55
    ),
    (
      'ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_WEST_OREGON',
      date('2023-11-01'),
      date('2024-11-01'),
      0.595 -- $0.7/dbu and we get a 15% discount. Not sure why its more expensive this year than last.
    ),
    (
      'ENTERPRISE_SERVERLESS_SQL_COMPUTE_US_WEST_OREGON',
      date('2024-11-01'),
      date('2027-10-29'),
      0.476 -- $0.7/dbu and we get a 32% discount.
    ),
    (
      'ENTERPRISE_SERVERLESS_SQL_COMPUTE_EUROPE_IRELAND',
      date('2021-11-01'),
      date('2023-11-01'),
      0.55
    ),
    (
      'ENTERPRISE_SERVERLESS_SQL_COMPUTE_EUROPE_IRELAND',
      date('2023-11-01'),
      date('2024-11-01'),
      0.7735 -- $0.91/dbu and we get a 15% discount. Not sure why its more expensive this year than last.
    ),
    (
      'ENTERPRISE_SERVERLESS_SQL_COMPUTE_EUROPE_IRELAND',
      date('2024-11-01'),
      date('2027-10-29'),
      0.6188 -- $0.91/dbu and we get a 32% discount.
    ),
    (
      'ENTERPRISE_SERVERLESS_SQL_COMPUTE_CANADA',
      date('2024-07-03'),
      date('2024-11-01'),
      0.6715 -- $0.79/dbu and we get a 15% discount.
    ),
    (
      'ENTERPRISE_DLT_CORE_COMPUTE',
      date('2023-11-01'),
      date('2024-11-01'),
      0.17 -- $0.20/dbu and we get a 15% discount.
    ),
    (
      'ENTERPRISE_DLT_CORE_COMPUTE',
      date('2024-11-01'),
      date('2027-10-29'),
      0.17 -- $0.20/dbu and we get a 15% discount.
    ),
    (
      'ENTERPRISE_DLT_ADVANCED_COMPUTE',
      date('2023-11-01'),
      date('2024-11-01'),
      0.17 -- $0.20/dbu and we get a 15% discount.
    ),
    -- There is a serverless compute feature used by Lakehouse Monitoring
    -- associated with this SKU.
    (
      'ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON',
      date('2023-11-01'),
      date('2024-11-01'),
      0.35
    ),
    (
      'ENTERPRISE_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON',
      date('2024-11-01'),
      date('2027-10-29'),
      0.306 -- $0.45/dbu and we get a 32% discount.
    ),
    (
      'ENTERPRISE_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND',
      date('2025-04-30'),
      current_date(),
      0.34
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_CANADA',
      date('2025-05-01'),
      current_date(),
      0.68
    ),
    (
      'ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_CANADA',
      date('2024-10-30'),
      current_date(),
      0.0624 -- according to system.billing.account_prices
    ),
    (
      'ENTERPRISE_JOBS_SERVERLESS_COMPUTE_CANADA',
      date('2025-04-30'),
      current_date(),
      0.3196
    ),
    (
      'ENTERPRISE_SERVERLESS_SQL_COMPUTE_CANADA',
      date('2024-11-01'),
      current_date(),
      0.79
    ),
    (
      'ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_US_WEST_OREGON',
      date('2023-11-01'),
      current_date(),
      0.07 -- TODO: do we get a discount here? This is the price from system.billing.list_prices
    ),
    (
      'ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_EUROPE_IRELAND',
      date('2023-11-01'),
      current_date(),
      0.078 -- TODO: do we get a discount here? This is the price from system.billing.list_prices
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_US_WEST_OREGON',
      date('2023-11-01'),
      date('2024-11-01'),
      0.95
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_US_WEST_OREGON',
      date('2024-11-01'),
      date('2027-10-29'),
      0.646 -- $0.95/dbu and we get a 32% discount.
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_EUROPE_IRELAND',
      date('2023-11-01'),
      date('2024-11-01'),
      1
    ),
    (
      'ENTERPRISE_ALL_PURPOSE_SERVERLESS_COMPUTE_EUROPE_IRELAND',
      date('2024-11-01'),
      date('2027-10-29'),
      0.68 -- $1/dbu and we get a 32% discount.
    ),
    (
      'ENTERPRISE_ANTHROPIC_MODEL_SERVING',
      date('2024-11-01'),
      date('2027-10-29'),
      0.07
    ),
    (
      'ENTERPRISE_SQL_PRO_COMPUTE_US_WEST_OREGON',
      date('2024-11-01'),
      date('2027-10-29'),
      0.374
    ),
    (
      'ENTERPRISE_DATABASE_SERVERLESS_COMPUTE_US_WEST_OREGON',
      date('2024-11-01'),
      date('2027-10-29'),
      0.26 -- according to system.billing.account_prices
    ),
    (
      'ENTERPRISE_MODEL_TRAINING_US_WEST_OREGON',
      date('2024-10-30'),
      current_date(),
      0.52 -- according to system.billing.account_prices
    ),
    (
      'ENTERPRISE_MODEL_TRAINING_EUROPE_IRELAND',
      date('2024-10-30'),
      current_date(),
      0.624 -- according to system.billing.account_prices
    ),
    (
      'ENTERPRISE_OPENAI_MODEL_SERVING',
      date('2025-09-23'),
      current_date(),
      0.07 -- according to system.billing.account_prices
    ),
    (
      'ENTERPRISE_GEMINI_MODEL_SERVING',
      date('2025-12-09'),
      current_date(),
      0.070000000000000000 -- according to system.billing.account_prices
    )
    AS (sku, start,end,price)
),

workspaces AS (
  VALUES
    (8972003451708087, 'R&D Legacy US', 'us-west-2', 'dataplatform'),
    (4378812686121663, 'R&D Legacy EU', 'eu-west-1', 'dataplatform'),
    (5924096274798303, 'R&D Dev US', 'us-west-2', 'dataplatform'),
    (6992178240159315, 'R&D Dev EU', 'eu-west-1', 'dataplatform'),
    (5936507695035553, 'R&D Prod US', 'us-west-2', 'dataplatform'),
    (1976292512529253, 'R&D Dev CA', 'ca-central-1', 'dataplatform'),
    (2510602084754375, 'R&D Staging US', 'us-west-2', 'dataplatform'),
    (7702501906276199, 'BizTech US', 'us-west-2', 'biztech'),
    (5058836858322111, 'BizTech POC 1', 'us-west-2', 'biztech')
    AS (workspaceId, workspaceName, region, workspaceowner)
),

usage AS (
  SELECT
    bill.workspaceId,
    workspaces.workspaceName,
    workspaces.workspaceowner,
    workspaces.region,
    timestamp,
    clusterId,
    clusterNodeType,
    bill.sku,
    dbus,
    machineHours,
    CASE
      WHEN get_json_object(clusterCustomTags, '$.samsara:team') is not null THEN lower(
        get_json_object(clusterCustomTags, '$.samsara:team')
      )
      WHEN get_json_object(clusterCustomTags, '$.samsara:pooled-job:team') is not null THEN lower(
        get_json_object(clusterCustomTags, '$.samsara:pooled-job:team')
      ) -- Check if we have a pooled tag
      -- if the usage comes from the biztech workspaces, set the team to "biztech"
      WHEN workspaces.workspaceowner = "biztech" THEN "biztech"
      ELSE "unknown" -- Use 'unknown' as a fallback
    END AS team,
    CASE
      WHEN get_json_object(clusterCustomTags, '$.samsara:product-group') is not null
      and get_json_object(clusterCustomTags, '$.samsara:product-group') != "" THEN lower(
        get_json_object(clusterCustomTags, '$.samsara:product-group')
      )
      WHEN get_json_object(
        clusterCustomTags,
        '$.samsara:pooled-job:product-group'
      ) is not null
      and get_json_object(
        clusterCustomTags,
        '$.samsara:pooled-job:product-group'
      ) != "" THEN lower(
        get_json_object(
          clusterCustomTags,
          '$.samsara:pooled-job:product-group'
        )
      )
      -- if the usage comes from the biztech workspaces, set the product group to "biztech"
      WHEN workspaces.workspaceowner = "biztech" THEN "biztech"
      ELSE "unknown" -- Use 'unknown' as a fallback
    END AS productGroup,
    prices.price,
    CASE
      WHEN get_json_object(clusterCustomTags, '$.samsara:service') is not null THEN lower(
        get_json_object(clusterCustomTags, '$.samsara:service')
      )
      WHEN get_json_object(
        clusterCustomTags,
        '$.samsara:pooled-job:service'
      ) is not null THEN lower(
        get_json_object(
          clusterCustomTags,
          '$.samsara:pooled-job:service'
        )
      )
      ELSE 'unknown'
    END AS service,
    clusterCustomTags,
    CASE
      WHEN get_json_object(clusterCustomTags, '$.samsara:rnd-allocation') is not null THEN cast(
        get_json_object(clusterCustomTags, '$.samsara:rnd-allocation') as double
      )
      WHEN get_json_object(
        clusterCustomTags,
        '$.samsara:pooled-job:rnd-allocation'
      ) is not null then cast(
        get_json_object(
          clusterCustomTags,
          '$.samsara:pooled-job:rnd-allocation'
        ) as double
      )
      ELSE NULL
    END AS rndAllocation,
    CASE
      WHEN get_json_object(clusterCustomTags, '$.samsara:dataplatform-job-type') is not null THEN lower(
        get_json_object(clusterCustomTags, '$.samsara:dataplatform-job-type')
      )
      WHEN get_json_object(
        clusterCustomTags,
        '$.samsara:pooled-job:dataplatform-job-type'
      ) is not null THEN lower(
        get_json_object(
          clusterCustomTags,
          '$.samsara:pooled-job:dataplatform-job-type'
        )
      )
      ELSE 'unknown'
    END AS dataplatformJobType,
    get_json_object(
      clusterCustomTags,
      '$.samsara:pooled-job:pool-name'
    ) AS poolName
  FROM
    billing.databricks_cost_with_serverless_datapipelines as bill
    LEFT JOIN prices ON bill.sku = prices.sku
    AND prices.start <= bill.timestamp
    AND prices.end > bill.timestamp
  LEFT OUTER JOIN workspaces ON bill.workspaceId = workspaces.workspaceId
),

usage_bill_with_allocation_exploded AS (
  -- First query in the UNION: only handle rows where rndAllocation is set
  SELECT
    u.*,
    -- This splits a given row into 2 rows: one with the COGS allocation and one with the R&D allocation.
    explode(map("COGS", 1 - rndAllocation, "R&D", rndAllocation)) as (costAllocation, costAllocationPercentage)
  FROM
    usage u
  WHERE u.rndAllocation is not null -- rndAllocation is null when the usage was created before we added the rnd-allocation tag.

  UNION ALL

  -- Second query in the UNION: only handle rows where rndAllocation is not set
  SELECT
    u.*,
    CASE
      -- rndAllocation is null when the usage was created before we added the rnd-allocation tag. Hence we fallback
      -- to using what our logic was before where all automated/jobs compute is COGS and the rest is R&D.
      WHEN u.sku LIKE '%AUTOMATED%' OR u.sku LIKE '%JOBS%' THEN 'COGS'
      ELSE 'R&D'
    END AS costAllocation,
    1 as costAllocationPercentage -- for these older jobs without the rndAllocation tag, the entire cost goes towards the given allocation
  FROM
    usage u
  WHERE
    u.rndAllocation is null
),

usage_bill_with_zeroed_out_allocations_removed AS (
  SELECT
    u.*
  FROM
    usage_bill_with_allocation_exploded u
  WHERE
    u.costAllocationPercentage != 0 -- Remove any rows where COGS or R&D allocation are 0 (meaning the cost is entirely R&D or entirely COGS)
)

SELECT
  u.workspaceId,
  u.workspaceName,
  u.workspaceowner,
  u.region,
  u.timestamp,
  u.clusterId,
  u.clusterNodeType,
  u.sku,
  u.dbus as totalDbusNotSplit, -- represents the total # of dbus for the job/cluster, not split by COGS vs R&D if the job ends up being split between the two
  u.machineHours as totalMachineHoursNotSplit, -- represents the total # of machine hours for the job/cluster, not split by COGS vs R&D if the job ends up being split between the two
  u.team,
  u.productGroup,
  u.service,
  u.clusterCustomTags,
  u.dataplatformJobType,
  -- Figure out the "dataplatformfeature", which is a broad grouping of various job types.
  -- e.g. group all rds jobs (merge, combine shards, vacuum, etc.) under "rds_replication".
  CASE
    WHEN u.dataplatformjobtype LIKE 'rds_deltalake_%' THEN 'rds_replication'
    WHEN u.dataplatformjobtype LIKE 'kinesisstats_deltalake_%' THEN 'ks_replication'
    WHEN u.dataplatformjobtype LIKE 'kinesisstats_bigstats_deltalake_%' THEN 'ks_big_stats_replication'
    WHEN u.dataplatformjobtype LIKE 'report_%' THEN 'reports'
    WHEN u.dataplatformjobtype LIKE 'data_pipelines_%' THEN 'data_pipelines'
    WHEN u.dataplatformjobtype = 'sql_endpoint' THEN 'sql_endpoint'
    WHEN u.dataplatformjobtype = 'scheduled_notebook' AND u.service LIKE 'databricksjob-backend_dataplatform_data_streams_live_ingestion%'  THEN 'data_streams'
    --Now handle all the cases where job type is unknown (job was created before certain date when we added that column) and try to guess what it is based on tags
    WHEN u.dataplatformjobtype = 'unknown' AND u.service LIKE 'databricksjob-kinesisstats%' THEN 'ks_replication'
    WHEN u.dataplatformjobtype = 'unknown' AND u.service LIKE 'databricksjob-rds%' THEN 'rds_replication'
    WHEN u.dataplatformjobtype = 'unknown' AND u.service LIKE 'databricksjob-s3bigstats%' THEN 'ks_big_stats_replication'
    WHEN u.dataplatformjobtype = 'unknown' AND (u.service LIKE 'data-pipeline-%' OR get_json_object(u.clusterCustomTags, '$.samsara:pipeline-node-name') is not null) THEN 'data_pipelines'
    WHEN u.dataplatformjobtype = 'unknown' AND u.service LIKE 'databricksjob-report-%' THEN 'reports'
    WHEN u.dataplatformjobtype = 'unknown' AND u.service LIKE 'databricksjobcluster-%' THEN 'scheduled_notebook_not_in_repo'
    WHEN u.dataplatformjobtype = 'unknown' AND u.service LIKE 'databricksjob-backend_%' THEN 'scheduled_notebook'
    WHEN u.dataplatformjobtype = 'unknown' AND (u.sku LIKE '%ALL_PURPOSE%' OR u.service LIKE 'databrickscluster-%') THEN 'interactive_cluster'
    WHEN u.dataplatformjobtype = 'unknown' AND (u.sku LIKE '%SQL_COMPUTE%' OR u.service LIKE 'databrickssql-%') THEN 'sql_endpoint'
    WHEN u.dataplatformjobtype = 'unknown' AND u.service LIKE '%dagster%' THEN 'dagster'
    ELSE u.dataplatformjobtype
  END AS dataplatformfeature,
  u.poolName,
  u.rndAllocation > 0 and u.rndAllocation < 1 AS isCostAllocationSplit,
  u.costAllocationPercentage * u.dbus as dbus,
  u.costAllocationPercentage * u.machineHours as machineHours,
  u.costAllocation,
  u.price * u.dbus * u.costAllocationPercentage as dbuCosts
FROM
  usage_bill_with_zeroed_out_allocations_removed u
)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("""
-- MAGIC   select
-- MAGIC     sku,
-- MAGIC     min(to_date(timestamp)) as earliest_use,
-- MAGIC     max(to_date(timestamp)) as latest_use
-- MAGIC   from
-- MAGIC     billing.databricks_rollup
-- MAGIC   where
-- MAGIC     dbucosts is null
-- MAGIC   group by 1
-- MAGIC   order by 2 asc
-- MAGIC """)
-- MAGIC
-- MAGIC if df.count() > 0:
-- MAGIC   print("There are some SKUs that have no associated pricing information, please add them to the prices CTE. Unknown SKUs: ")
-- MAGIC   display(df)
-- MAGIC   raise Exception("Failing on unknown SKUs")
-- MAGIC else:
-- MAGIC   print("Found no invalid skus.")
