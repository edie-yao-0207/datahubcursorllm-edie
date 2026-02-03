WITH manual_odometers AS (
  SELECT
    id as device_id,
    org_id,
    manual_odometer_updated_at,
    manual_odometer_meters,
    DATE_FORMAT(manual_odometer_updated_at, 'yyyy-MM-dd') AS date
  FROM productsdb.devices
  WHERE manual_odometer_meters > 0
    AND DATE_FORMAT(manual_odometer_updated_at, 'yyyy-MM-dd') >= ${start_date}
    AND DATE_FORMAT(manual_odometer_updated_at, 'yyyy-MM-dd') < ${end_date}
),

gps_distances AS (
SELECT
  org_id,
  object_id,
  time,
  value.double_value AS gps_distance
FROM kinesisstats.osdderivedgpsdistance
WHERE date >= DATE_SUB(${start_date}, 60)
  AND date < ${end_date}
  AND object_id IN (
    SELECT device_id
    FROM manual_odometers
  )
)

SELECT
  m.org_id,
  device_id,
  date,
  manual_odometer_meters,
  manual_odometer_updated_at,
  COALESCE(CAST(MAX(gps_distance) AS BIGINT), 0) AS gps_distance_at_manual_update
FROM manual_odometers AS m
LEFT JOIN gps_distances AS gps
  ON m.device_id = gps.object_id
  AND m.org_id = gps.org_id
  AND gps.time <= CAST(to_unix_timestamp(manual_odometer_updated_at) AS BIGINT) * 1000
GROUP BY
  device_id,
  m.org_id,
  date,
  manual_odometer_meters,
  manual_odometer_updated_at
