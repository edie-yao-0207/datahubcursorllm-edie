SELECT
  CAST(csb.org_id AS BIGINT) AS org_id,
  DATE(cs.completed_date) AS date,
  csb.driver_id AS user_id,
  csb.coaching_session_uuid
FROM
  coachingdb_shards.coaching_sessions_behavior csb
JOIN coachingdb_shards.coaching_sessions cs
  ON csb.org_id = cs.org_id
  AND csb.coaching_session_uuid = cs.uuid
WHERE
  coachable_item_type = 43 -- Idling
  AND csb.driver_id != 0
  AND csb.driver_id IS NOT NULL
