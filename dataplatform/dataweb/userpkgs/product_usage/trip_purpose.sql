SELECT
  CAST(org_id AS BIGINT) AS org_id,
  date,
  driver_id AS user_id
FROM
  tripsdb_shards.trip_purpose_assignments
WHERE
  trip_purpose != 0 -- Not unset
