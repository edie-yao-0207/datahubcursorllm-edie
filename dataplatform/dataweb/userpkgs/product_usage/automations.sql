WITH workflow_runs_us AS (
    -- Fetch workflow runs from last 56 days for determining which coaching automations have run
    SELECT DISTINCT
      date,
      workflow_config_id,
      org_id
    FROM datastreams_history.dynamic_workflow_runs
),
automation_events_us AS (
    -- Usage is defined as orgs with at least one enabled automation that has run within the timeframe
    SELECT
      CAST(a.orgid AS BIGINT) AS org_id,
      a.id AS configuration_id,
      a.owneruserid AS user_id
    FROM dynamodb.automation_configurations a
    JOIN clouddb.organizations o
        ON a.orgid = o.id
    WHERE
      a.orgid IS NOT NULL
      AND a.deleted = 'false'
      AND a.enabled = 'true'
)
SELECT
  w.date AS date,
  e.org_id,
  e.user_id,
  e.configuration_id,
  CONCAT(e.org_id, '_', e.configuration_id) AS usage_id
FROM workflow_runs_us w
JOIN automation_events_us e
  ON w.org_id = e.org_id
  AND w.workflow_config_id = e.configuration_id
