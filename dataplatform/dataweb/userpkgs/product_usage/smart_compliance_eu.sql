SELECT
  org_id,
  date,
  uuid,
  driver_id AS user_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coachingdb/coachingdb/coachable_item_v0`
WHERE item_type = 7  -- EU_HOS_INFRINGEMENT_EVENT
  AND coachable_item_type = 63  -- EU_HOS_INFRINGEMENT
  AND coaching_state IN (10, 100)  -- NEEDS_COACHING or COACHED

UNION ALL

SELECT
  org_id,
  date,
  uuid,
  driver_id AS user_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-coaching-shard-1db/coachingdb/coachable_item_v0`
WHERE item_type = 7  -- EU_HOS_INFRINGEMENT_EVENT
  AND coachable_item_type = 63  -- EU_HOS_INFRINGEMENT
  AND coaching_state IN (10, 100)  -- NEEDS_COACHING or COACHED
