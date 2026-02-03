SELECT
  CAST(org_id AS BIGINT) AS org_id,
  DATE(completed_date) AS date,
  driver_id AS user_id,
  uuid AS coaching_session_uuid
FROM
  coachingdb_shards.coaching_sessions
WHERE
  session_type IN (1, 2) -- Manager-Led Coaching
