-- Databricks notebook: https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/263855033555242/command/263855033555243
WITH trips_bucket_end_time AS (
  SELECT
    org_id,
    from_unixtime(proto.end.time / 1000, "yyyy-MM-dd") as date,
    start_ms,
    device_id,
    driver_id,
    date_trunc('hour', from_unixtime(proto.end.time / 1000)) AS interval_start,
    date_trunc('hour', from_unixtime(proto.end.time / 1000)) + interval 1 hour AS interval_end,
    STRUCT(
        proto.start.time AS start_ms,
        proto.end.time AS end_ms,
        proto.trip_distance.distance_meters AS distance_meters,
        proto.trip_odometers.start_odometer AS start_odometer,
        proto.trip_odometers.end_odometer AS end_odometer,
        proto.start.place AS start_place,
        proto.end.place AS end_place
    ) AS trip
  FROM trips2db_shards.trips
  WHERE date >= date_add(${start_date}, -2) AND date < ${end_date}
    AND (proto.ongoing IS NULL OR NOT proto.ongoing)
    AND proto.trip_distance.distance_meters > 0
    AND version = 101
),

engine_ms AS (
  SELECT *,
  CASE WHEN CAST(from_unixtime(trip.end_ms / 1000) AS TIMESTAMP) > interval_end
    THEN (CAST(to_unix_timestamp(interval_end) AS BIGINT) * 1000) - trip.start_ms
    ELSE trip.end_ms - trip.start_ms END
    AS engine_ms
  FROM trips_bucket_end_time
  ORDER BY start_ms ASC
),

report_rows as (
  SELECT
    org_id,
    device_id,
    driver_id,
    date,
    interval_start,
    SUM(trip.distance_meters) AS gps_distance_m,
    MIN(trip.start_odometer) AS start_odometer_m,
    MAX(trip.end_odometer) AS end_odometer_m,
    COUNT(*) AS stops_count,
    SUM(engine_ms) AS drive_time_ms,

    -- Determine start/end bounds of first and last trips in the timerange
    MIN(STRUCT("start_ms", trip.start_ms, "end_ms", trip.end_ms)).start_ms AS first_trip_start_ms,
    MIN(STRUCT("start_ms", trip.start_ms, "end_ms", trip.end_ms)).end_ms AS first_trip_end_ms,
    MAX(STRUCT("start_ms", trip.start_ms, "end_ms", trip.end_ms)).start_ms AS last_trip_start_ms,
    MAX(STRUCT("start_ms", trip.start_ms, "end_ms", trip.end_ms)).end_ms AS last_trip_end_ms
  FROM engine_ms
  WHERE date >= ${start_date} and date < ${end_date}
  GROUP BY org_id, device_id, driver_id, date, interval_start
)

SELECT * FROM report_rows
