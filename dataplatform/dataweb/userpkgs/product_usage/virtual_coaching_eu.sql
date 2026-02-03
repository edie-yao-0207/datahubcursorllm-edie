WITH coaching_sessions_eu AS (
  SELECT
    org_id,
    uuid
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coaching_sessions_v0`

  UNION ALL

  SELECT
    org_id,
    uuid
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coaching_sessions_v0`
),
coachable_item_eu AS (
  SELECT
    org_id,
    coaching_session_uuid,
    uuid
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coachable_item_v0`

  UNION ALL

  SELECT
    org_id,
    coaching_session_uuid,
    uuid
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_v0`
),
coachable_item_share_eu AS (
  SELECT
    org_id,
    driver_id,
    shared_at,
    share_type,
    coachable_item_uuid
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coachable_item_share_v0`

  UNION ALL

  SELECT
    org_id,
    driver_id,
    shared_at,
    share_type,
    coachable_item_uuid
  FROM
    delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_share_v0`
)
SELECT
  CAST(cis.org_id AS BIGINT) AS org_id,
  DATE(cis.shared_at) AS date,
  cis.driver_id AS user_id,
  cs.uuid AS coaching_session_uuid
FROM
  coaching_sessions_eu cs
JOIN coachable_item_eu ci
  ON cs.uuid = ci.coaching_session_uuid
JOIN coachable_item_share_eu cis
  ON ci.uuid = cis.coachable_item_uuid
WHERE
  cis.share_type = 1 -- Self-Coaching
