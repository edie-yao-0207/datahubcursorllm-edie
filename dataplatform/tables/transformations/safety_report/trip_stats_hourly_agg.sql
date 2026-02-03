WITH trip_stats AS (
  SELECT
    *,
    WINDOW (
    --TODO: use end_ms after we materialize safety_report.trip_speeding.end_ms
    FROM_UNIXTIME(end_ms / CAST (1e3 AS DECIMAL (4, 0))),
    '1 hour'
).start AS interval_start
FROM
  safety_report.trip_speeding
WHERE date >= ${start_date}
  AND date < ${end_date}
),

device_trip AS (
  SELECT
    org_id,
    device_id AS object_id,
    1 AS object_type,
    *
  FROM
    trip_stats
),

driver_trip AS (
  SELECT
    org_id,
    COALESCE(driver_id, 0) AS object_id,
    5 AS object_type,
    *
  FROM
    trip_stats
),

object_trip_stats AS (
  SELECT
    *
  FROM
    device_trip
  UNION ALL
  SELECT
    *
  FROM
    driver_trip
)

SELECT
  org_id,
  object_id,
  object_type,
  version,
  date,
  interval_start,
  count(*) AS trip_count,
  sum(not_speeding_count) AS not_speeding_count,
  sum(light_speeding_count) AS light_speeding_count,
  sum(moderate_speeding_count) AS moderate_speeding_count,
  sum(heavy_speeding_count) AS heavy_speeding_count,
  sum(severe_speeding_count) AS severe_speeding_count,
  sum(not_speeding_ms) AS not_speeding_ms,
  sum(light_speeding_ms) AS light_speeding_ms,
  sum(moderate_speeding_ms) AS moderate_speeding_ms,
  sum(heavy_speeding_ms) AS heavy_speeding_ms,
  sum(severe_speeding_ms) AS severe_speeding_ms,
  sum(engine_idle_ms) AS engine_idle_ms,
  sum(distance_meters) AS distance_meters,
  sum(driving_time_ms) AS driving_time_ms,
  sum(seatbelt_reporting_present_ms) AS seatbelt_reporting_present_ms,
  sum(seatbelt_unbuckled_ms) AS seatbelt_unbuckled_ms,
  -- Calculate distance metrics for camera-equipped vehicles
  sum(CASE WHEN has_outward_camera = 1 THEN distance_meters ELSE 0 END) AS outward_camera_distance_meters,
  sum(CASE WHEN has_inward_outward_camera = 1 THEN distance_meters ELSE 0 END) AS inward_outward_camera_distance_meters,
  -- Calculate driving time metrics for camera-equipped vehicles
  sum(CASE WHEN has_outward_camera = 1 THEN driving_time_ms ELSE 0 END) AS outward_camera_driving_time_ms,
  sum(CASE WHEN has_inward_outward_camera = 1 THEN driving_time_ms ELSE 0 END) AS inward_outward_camera_driving_time_ms
FROM
  object_trip_stats
GROUP BY
  org_id,
  object_id,
  object_type,
  version,
  date,
  interval_start
