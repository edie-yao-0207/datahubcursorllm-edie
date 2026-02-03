SELECT
  CAST(org_id AS BIGINT) AS org_id,
  TO_DATE(created_at) AS date,
  shared_by_id AS user_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/positive_recognition_shares_v0` prs
WHERE
  org_id IS NOT NULL

UNION ALL

SELECT
  CAST(org_id AS BIGINT) AS org_id,
  TO_DATE(created_at) AS date,
  shared_by_id AS user_id
FROM
  delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/positive_recognition_shares_v0` prs
WHERE
  org_id IS NOT NULL
