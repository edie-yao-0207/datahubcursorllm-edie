WITH vanguard_locations_us AS (
  SELECT
    l.device_id,
    l.org_id,
    l.value.latitude AS latitude,
    l.value.longitude AS longitude,
    l.date
  FROM
    kinesisstats_history.location l
  JOIN hardware_analytics.vanguard_devices v
    ON l.device_id = v.device_id
    AND l.org_id = v.org_id
  JOIN final_relevant_orgs rl
    ON l.org_id = rl.org_id
  WHERE l.date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}'
),
date_sequence_us AS (
  SELECT
    EXPLODE(
      SEQUENCE(
        DATE_SUB('{PARTITION_START}', 27),
        DATE('{PARTITION_START}'),
        INTERVAL 1 DAY
      )
    ) AS date_sequence
),
device_bounds_us AS (
  SELECT
    device_id,
    org_id,
    MIN(latitude) AS min_lat,
    MAX(latitude) AS max_lat,
    MIN(longitude) AS min_lon,
    MAX(longitude) AS max_lon
  FROM vanguard_locations_us
  GROUP BY
    device_id,
    org_id
)
SELECT
  CAST(s.OrgId AS BIGINT) AS org_id,
  s.date AS date,
  s.AssetId AS device_id,
  s.WorkerPolymorphicUserId AS user_id
FROM dynamodb.worker_safety_incidents s
WHERE
  s.OrgId IS NOT NULL
  AND s.AssetId IS NOT NULL

UNION ALL

SELECT
  d.org_id,
  date_sequence AS date,
  d.device_id,
  a.PolymorphicAssigneeId AS user_id
FROM device_bounds_us d
LEFT OUTER JOIN dynamodb.worker_safety_wearable_assignments a
  ON d.device_id = a.DeviceId
  AND d.org_id = CAST(a.OrgId AS BIGINT)
  AND (
    DATE(FROM_UNIXTIME(a.EndMs / 1000)) = '1970-01-01'
    OR DATE(FROM_UNIXTIME(a.EndMs / 1000)) >= DATE_SUB('{PARTITION_START}', 27)
  )
CROSS JOIN date_sequence_us
WHERE 3959 * 2 * ASIN(SQRT(
      POWER(SIN(RADIANS((d.max_lat - d.min_lat) / 2)), 2) +
      COS(RADIANS(d.min_lat)) * COS(RADIANS(d.max_lat)) *
      POWER(SIN(RADIANS((d.max_lon - d.min_lon) / 2)), 2)
    )) > 0.5
