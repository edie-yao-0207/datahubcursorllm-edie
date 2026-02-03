WITH trips as (
  SELECT
    (end_ms - start_ms) / 60000 as duration_minutes,
    end_ms,
    start_ms,
    device_id,
    org_id,
    date
  FROM trips2db_shards.trips
  WHERE device_id IN (SELECT * FROM apptelematics.device_ids)
  AND date >= ${start_date} AND date < ${end_date}
  AND version = 101
),

points as (
  SELECT
    org_id,
    device_id,
    value.time loc_timestamp,
    value.gps_speed_meters_per_second speed,
    value.accuracy_millimeters
  FROM kinesisstats.location
  WHERE device_id in (SELECT * FROM apptelematics.device_ids)
  AND date >= ${start_date} AND date < ${end_date}
),

trips_with_num_points as (
  SELECT
    trips.org_id org_id,
    trips.device_id device_id,
    duration_minutes,
    start_ms,
    end_ms,
    date,
    count(loc_timestamp) num_points,
    avg(speed) * 2.23694 as avg_speed_mph, -- m/s to mph
    avg(accuracy_millimeters)/1000 as avg_accuracy_m
  FROM points
  JOIN trips
  WHERE trips.org_id = points.org_id
  AND trips.device_id = points.device_id
  AND loc_timestamp >= start_ms
  AND loc_timestamp < end_ms
  GROUP by trips.org_id, trips.device_id, duration_minutes, start_ms, end_ms, date
),

trip_metrics AS (
  SELECT
    date,
    name AS org_name,
    org_id,
    device_id,
    start_ms,
    FROM_UTC_TIMESTAMP(from_unixtime(start_ms/1000), "America/Los_Angeles") trip_start_pacific,
    duration_minutes duration_mins,
    num_points/duration_minutes points_per_min,
    avg_speed_mph,
    avg_accuracy_m,
    FORMAT_STRING("https://cloud.samsara.com/o/%d/devices/%d/vehicle?end_ms=%d&trip=%d&autoPan=false", org_id, device_id, end_ms+1000*60*60, end_ms) AS url
  FROM trips_with_num_points
  JOIN clouddb.organizations
  ON organizations.id = trips_with_num_points.org_id
  ORDER BY end_ms DESC
)

SELECT
  DATE(tm.date) AS date,
  tm.org_name,
  tm.device_id,
  tpa.mobile_platform AS mobile_platform,
  tpa.mobile_os_version AS mobile_os_version,
  tpa.mobile_bundle_version as mobile_bundle_version,
  tm.trip_start_pacific,
  tm.duration_mins,
  tm.points_per_min,
  tm.avg_speed_mph,
  tm.avg_accuracy_m,
  tm.url
FROM trip_metrics tm
LEFT JOIN apptelematics.trip_platform_assignment tpa
ON tm.org_id = tpa.org_id
AND tm.device_id = tpa.device_id
AND tm.start_ms = tpa.trip_start_ms
