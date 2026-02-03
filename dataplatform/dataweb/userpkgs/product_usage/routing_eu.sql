WITH route_daily_events_eu AS (
  SELECT
    CAST(org_id AS BIGINT) AS org_id,
    DATE(timestamp_millis(scheduled_start_ms)) AS date
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-dispatchdb/dispatchdb/shardable_dispatch_routes_v1`
  WHERE
    org_id IS NOT NULL

  UNION ALL

  SELECT
    CAST(org_id AS BIGINT) AS org_id,
    DATE(timestamp_millis(scheduled_start_ms)) AS date
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-dispatch-shard-1db/dispatchdb/shardable_dispatch_routes_v1`
  WHERE
    org_id IS NOT NULL
),
job_daily_events_eu AS (
  -- This grabs the job events
  SELECT
    CAST(org_id AS BIGINT) AS org_id,
    TO_DATE(date) AS date
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-dispatchdb/dispatchdb/shardable_dispatch_jobs_v1`
  WHERE
    org_id IS NOT NULL

  UNION ALL

  SELECT
    CAST(org_id AS BIGINT) AS org_id,
    TO_DATE(date) AS date
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-dispatch-shard-1db/dispatchdb/shardable_dispatch_jobs_v1`
  WHERE
    org_id IS NOT NULL
)
SELECT org_id, date FROM route_daily_events_eu

UNION ALL

SELECT org_id, date FROM job_daily_events_eu
