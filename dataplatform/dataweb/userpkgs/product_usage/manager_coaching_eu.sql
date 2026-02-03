SELECT
  CAST(org_id AS BIGINT) AS org_id,
  DATE(completed_date) AS date,
  driver_id AS user_id,
  uuid AS coaching_session_uuid
FROM
  delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coaching_sessions_v0`
WHERE
  session_type IN (1, 2) -- Manager-Led Coaching

UNION ALL

SELECT
  CAST(org_id AS BIGINT) AS org_id,
  DATE(completed_date) AS date,
  driver_id AS user_id,
  uuid AS coaching_session_uuid
FROM
  delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coaching_sessions_v0`
WHERE
  session_type IN (1, 2) -- Manager-Led Coaching
