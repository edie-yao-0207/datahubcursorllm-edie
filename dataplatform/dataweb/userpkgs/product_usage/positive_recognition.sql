SELECT
  CAST(org_id AS BIGINT) AS org_id,
  TO_DATE(created_at) AS date,
  shared_by_id AS user_id
FROM coachingdb_shards.positive_recognition_shares prs
WHERE
  org_id IS NOT NULL
