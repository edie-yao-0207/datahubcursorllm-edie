SELECT
  CAST(cis.org_id AS BIGINT) AS org_id,
  DATE(cis.shared_at) AS date,
  cis.driver_id AS user_id,
  cs.uuid AS coaching_session_uuid
FROM
  coachingdb_shards.coaching_sessions cs
JOIN coachingdb_shards.coachable_item ci
  ON cs.uuid = ci.coaching_session_uuid
JOIN coachingdb_shards.coachable_item_share cis
  ON ci.uuid = cis.coachable_item_uuid
WHERE
  cis.share_type = 1 -- Self-Coaching
