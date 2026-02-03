WITH coaching_sessions_eu AS (
  SELECT
    completed_date,
    org_id,
    uuid
  FROM
      delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coaching_sessions_v0`

  UNION ALL

  SELECT
    completed_date,
    org_id,
    uuid
  FROM
      delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coaching_sessions_v0`
),
coaching_sessions_behavior_eu AS (
  SELECT
    org_id,
    driver_id,
    coaching_session_uuid,
    coachable_item_type
  FROM
      delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coaching_sessions_behavior_v0`

  UNION ALL

  SELECT
    org_id,
    driver_id,
    coaching_session_uuid,
    coachable_item_type
  FROM
      delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coaching_sessions_behavior_v0`
)
SELECT
  CAST(csb.org_id AS BIGINT) AS org_id,
  DATE(cs.completed_date) AS date,
  csb.driver_id AS user_id,
  csb.coaching_session_uuid
FROM
  coaching_sessions_behavior_eu csb
JOIN coaching_sessions_eu cs
  ON csb.org_id = cs.org_id
  AND csb.coaching_session_uuid = cs.uuid
WHERE
  coachable_item_type = 42 -- HoS
  AND csb.driver_id != 0
  AND csb.driver_id IS NOT NULL
