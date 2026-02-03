-- Databricks notebook: https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/3222598258877083/command/3882378160075328
-- Select trips that will be used as basis of calculation and decorate them
-- with trip purpose data where available
WITH trips_bucket_end_time AS (
  SELECT
    t.org_id,
    from_unixtime(t.proto.end.time / 1000, "yyyy-MM-dd") as date,
    t.start_ms,
    t.device_id,
    t.driver_id,
    date_trunc('hour', from_unixtime(t.proto.end.time / 1000)) AS interval_start,
    date_trunc('hour', from_unixtime(t.proto.end.time / 1000)) + interval 1 hour AS interval_end,
    STRUCT(
        t.proto.start.time AS start_ms,
        t.proto.end.time AS end_ms,
        t.proto.trip_distance.distance_meters AS distance_meters,
        t.proto.trip_odometers.start_odometer AS start_odometer,
        t.proto.trip_odometers.end_odometer AS end_odometer,
        tp.trip_purpose AS trip_purpose
    ) AS trip
  FROM trips2db_shards.trips t
  LEFT JOIN tripsdb_shards.trip_purpose_assignments tp
    ON t.org_id = tp.org_id
    AND t.device_id = tp.device_id
    AND t.start_ms = tp.assignment_start_ms
  WHERE t.date >= date_add(${start_date}, -2) AND t.date < ${end_date}
    AND (t.proto.ongoing IS NULL OR NOT t.proto.ongoing)
    AND t.version = 101
),

engine_ms AS (
  SELECT *,
  CASE WHEN CAST(from_unixtime(trip.end_ms / 1000) AS TIMESTAMP) > interval_end
    THEN (CAST(to_unix_timestamp(interval_end) AS BIGINT) * 1000) - trip.start_ms
    ELSE trip.end_ms - trip.start_ms END
    AS engine_ms
  FROM trips_bucket_end_time
),

device_rows as (
  SELECT
    org_id AS org_id,
    device_id AS object_id,
    date,
    -- object_type = 1 indicates this row is for a device in the final table combining both drivers & devices
    1 AS object_type,
    interval_start AS interval_start,
    interval_end AS interval_end,
    SUM(trip.distance_meters) AS gps_distance_m,
    SUM(CASE WHEN trip.trip_purpose = 1 THEN trip.distance_meters ELSE 0 END) AS gps_distance_personal_m,
    SUM(CASE WHEN trip.trip_purpose = 2 THEN trip.distance_meters ELSE 0 END) AS gps_distance_work_m,
    SUM(CASE WHEN trip.trip_purpose = 3 THEN trip.distance_meters ELSE 0 END) AS gps_distance_commute_m,
    SUM(CASE WHEN trip.trip_purpose = 10 THEN trip.distance_meters ELSE 0 END) AS gps_distance_other_m,
    SUM(engine_ms) AS engine_ms,
    COUNT(*) AS stops_count,
    MAX(trip.start_ms) AS last_trip_start_ms
  FROM engine_ms
  WHERE date >= ${start_date} and date < ${end_date}
  GROUP BY org_id, device_id, date, interval_start, interval_end
),

driver_rows as (
  SELECT
    org_id AS org_id,
    driver_id AS object_id,
    date,
    -- object_type = 5 indicates this row is for a driver in the final table combining both drivers & devices
    5 AS object_type,
    interval_start AS interval_start,
    interval_end AS interval_end,
    SUM(trip.distance_meters) AS gps_distance_m,
    SUM(CASE WHEN trip.trip_purpose = 1 THEN trip.distance_meters ELSE 0 END) AS gps_distance_personal_m,
    SUM(CASE WHEN trip.trip_purpose = 2 THEN trip.distance_meters ELSE 0 END) AS gps_distance_work_m,
    SUM(CASE WHEN trip.trip_purpose = 3 THEN trip.distance_meters ELSE 0 END) AS gps_distance_commute_m,
    SUM(CASE WHEN trip.trip_purpose = 10 THEN trip.distance_meters ELSE 0 END) AS gps_distance_other_m,
    SUM(engine_ms) AS engine_ms,
    COUNT(*) AS stops_count,
    MAX(trip.start_ms) AS last_trip_start_ms
  FROM engine_ms
  WHERE date >= ${start_date} AND date < ${end_date}
  AND COALESCE(driver_id, 0) != 0
  GROUP BY org_id, driver_id, date, interval_start, interval_end
),

report_rows AS (
  SELECT * FROM device_rows WHERE date >= ${start_date} AND date < ${end_date}
  UNION ALL (
    SELECT * FROM driver_rows WHERE date >= ${start_date} AND date < ${end_date}
  )
)

SELECT * FROM report_rows
