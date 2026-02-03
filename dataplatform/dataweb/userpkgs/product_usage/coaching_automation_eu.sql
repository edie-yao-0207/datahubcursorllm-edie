WITH workflow_runs_eu AS (
  -- Fetch workflow runs from last 60 days for determining which coaching automations have run
  SELECT DISTINCT
    date,
    workflow_config_id,
    org_id
  FROM delta.`s3://samsara-eu-data-streams-delta-lake/dynamic_workflow_runs`
),
automation_events_eu AS (
  -- Usage is defined as orgs with at least one enabled coaching automation that has run within the timeframe
  SELECT
    CAST(a.orgid AS BIGINT) AS org_id,
    a.id AS configuration_id,
    a.owneruserid AS user_id
  FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/automation-configurations` a
  JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` o
    ON a.orgid = o.id
  WHERE
    a.orgid IS NOT NULL
    AND a.steps LIKE '%sendToCoachingActivity%'
    AND a.deleted = 'false'
    AND a.enabled = 'true'
)
SELECT
  w.date AS date,
  e.org_id,
  e.user_id,
  e.configuration_id,
  CONCAT(e.org_id, '_', e.configuration_id) AS usage_id
FROM workflow_runs_eu w
JOIN automation_events_eu e
  ON w.org_id = e.org_id
  AND w.workflow_config_id = e.configuration_id
