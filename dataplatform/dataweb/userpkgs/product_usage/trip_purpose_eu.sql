SELECT
  CAST(org_id AS BIGINT) AS org_id,
  date,
  driver_id AS user_id
FROM
  delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-trips-shard-1db/tripsdb/trip_purpose_assignments_v0`
WHERE
  trip_purpose != 0 -- Not unset

UNION ALL

SELECT
  CAST(org_id AS BIGINT) AS org_id,
  date,
  driver_id AS user_id
FROM
  delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-tripsdb/tripsdb/trip_purpose_assignments_v0`
WHERE
  trip_purpose != 0 -- Not unset
