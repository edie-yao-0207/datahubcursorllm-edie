WITH device_history_eu AS (
  SELECT
    gh.gateway_id,
    gh.device_id,
    g.serial,
    g.org_id,
    CAST(ROUND(CAST(gh.`timestamp` AS double) * 1000, 0) AS long) AS time
  FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/gateway_device_history_v0` AS gh
  JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-productsdb/prod_db/gateways_v0` AS g
    ON gh.gateway_id = g.id
  WHERE g.product_id = 223
),
vanguard_devices_eu AS (
  SELECT
    gateway_id,
    device_id,
    serial,
    org_id,
    time AS first_association_time,
    FROM_UNIXTIME(time / 1000) AS first_association_time_utc,
    (LEAD(time) OVER (PARTITION BY gateway_id ORDER BY time)) - 1 AS last_association_time,
    FROM_UNIXTIME((LEAD(time) OVER (PARTITION BY gateway_id ORDER BY time) - 1) / 1000) AS last_association_time_utc
  FROM device_history_eu
),
vanguard_locations_eu AS (
  SELECT
    l.device_id,
    l.org_id,
    l.value.latitude AS latitude,
    l.value.longitude AS longitude,
    l.date
  FROM
    delta.`s3://samsara-eu-kinesisstats-delta-lake/table/deduplicated/location` l
  JOIN vanguard_devices_eu v
    ON l.device_id = v.device_id
    AND l.org_id = v.org_id
  JOIN final_relevant_orgs rl
    ON l.org_id = rl.org_id
  WHERE l.date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}'
),
date_sequence_eu AS (
  SELECT
    EXPLODE(
      SEQUENCE(
        DATE_SUB('{PARTITION_START}', 27),
        DATE('{PARTITION_START}'),
        INTERVAL 1 DAY
      )
    ) AS date_sequence
),
device_bounds_eu AS (
  SELECT
    device_id,
    org_id,
    MIN(latitude) AS min_lat,
    MAX(latitude) AS max_lat,
    MIN(longitude) AS min_lon,
    MAX(longitude) AS max_lon
  FROM vanguard_locations_eu
  GROUP BY
    device_id,
    org_id
)
SELECT
  CAST(s.OrgId AS BIGINT) AS org_id,
  s.date AS date,
  s.AssetId AS device_id,
  s.WorkerPolymorphicUserId AS user_id
FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/worker-safety-incidents` s
WHERE
  s.OrgId IS NOT NULL
  AND s.AssetId IS NOT NULL

UNION ALL

SELECT
  d.org_id,
  date_sequence AS date,
  d.device_id,
  a.PolymorphicAssigneeId AS user_id
FROM device_bounds_eu d
LEFT OUTER JOIN delta.`s3://samsara-eu-dynamodb-delta-lake/table/worker-safety-wearable-assignments` a
  ON d.device_id = a.DeviceId
  AND d.org_id = CAST(a.OrgId AS BIGINT)
  AND (
    DATE(FROM_UNIXTIME(a.EndMs / 1000)) = '1970-01-01'
    OR DATE(FROM_UNIXTIME(a.EndMs / 1000)) >= DATE_SUB('{PARTITION_START}', 27)
  )
CROSS JOIN date_sequence_eu
WHERE 3959 * 2 * ASIN(SQRT(
      POWER(SIN(RADIANS((d.max_lat - d.min_lat) / 2)), 2) +
      COS(RADIANS(d.min_lat)) * COS(RADIANS(d.max_lat)) *
      POWER(SIN(RADIANS((d.max_lon - d.min_lon) / 2)), 2)
    )) > 0.5
