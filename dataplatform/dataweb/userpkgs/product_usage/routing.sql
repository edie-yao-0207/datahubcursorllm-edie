WITH route_daily_events_us AS (
  -- Usage is composed of route creation and job creation
  SELECT
    CAST(org_id AS BIGINT) AS org_id,
    DATE(timestamp_millis(scheduled_start_ms)) AS date
  FROM
    dispatchdb_shards.shardable_dispatch_routes
  WHERE
    org_id IS NOT NULL
),
job_daily_events_us AS (
  -- This grabs the job events
  SELECT
    CAST(org_id AS BIGINT) AS org_id,
    TO_DATE(date) AS date
  FROM
    dispatchdb_shards.shardable_dispatch_jobs
  WHERE
    org_id IS NOT NULL
)
SELECT org_id, date FROM route_daily_events_us

UNION ALL

SELECT org_id, date FROM job_daily_events_us
