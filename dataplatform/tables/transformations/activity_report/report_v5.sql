-- windowed_odometer is a table with each row annotated with its previous and
-- next entries. You have the row directly from filtered_odometer, as well as
-- the row immediately before it as 'previous', and the row immediately after it
-- as 'next'.  It reads all odo values within 30d before the start of the calculation
-- window to avoid falling back to synthetic odometer if there are no odometer
-- values within the current window. This matches the odometer calculation used
-- by the activity "details" page. (The ordering over the rows is determined by time_ms)
WITH windowed_odometer AS (
  SELECT *,
    LAG(STRUCT(*)) OVER (
      PARTITION BY org_id, device_id ORDER BY time_ms
    ) AS previous,
    LEAD(STRUCT(*)) OVER (
      PARTITION BY org_id, device_id ORDER BY time_ms
    ) AS next
  FROM canonical_distance.filtered_odometer
  WHERE date >= DATE_SUB(${start_date}, 30)
  AND date < ${end_date}
  AND valid = true
  AND time_ms <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000
),

-- exploded_osdodometer is a table that converts each row from windowed_odometer
-- and changes its time_ms to an interval_start, structifies the previous and
-- current values to only grab the fields we care about, and grabs all the rows
-- within the timerange [start_date, end_date).  For each row, we also write out
-- a row (explode) for each interval_start over the range [time_ms,
-- next.time_ms].  If next is null, we will write out interval_starts until
-- end_date inclusive. The following table osdodometer will filter out end_date.
-- We do this because it's totally possible for a device to not report odometer
-- in every hour over [start_date, end_date).  If a device doesn't report
-- odometer for a given interval_start, but it reports it in other
-- interval_starts, then it's possible that we might end up using synthetic
-- odometer when we shouldn't be. What this tries to do is populate all
-- interval_starts in [start_date, end_date) with odometer values so we don't
-- fall back to synthetic odometer.
--
-- Note: this code makes the assumption that for a vehicle that normally reports
-- odometer, if there exists gps values within a 3 day window, there will exist
-- at least 1 odometer value also within the window.  This situation is unlikely
-- but if it ends up impacting customers we would need to come up with a
-- different solution.
exploded_osdodometer AS (
  SELECT
    org_id,
    device_id,
    date,
    EXPLODE(
      SEQUENCE(
        CASE WHEN date >= ${start_date} AND previous IS NOT NULL THEN DATE_TRUNC('hour', FROM_UNIXTIME(time_ms / 1000))
        ELSE DATE_TRUNC('hour', ${start_date}) END,
        CASE WHEN next IS NOT NULL THEN DATE_TRUNC('hour', FROM_UNIXTIME(next.time_ms / 1000))
        ELSE DATE_TRUNC('hour', ${end_date}) END,
        INTERVAL 1 HOUR
      )
    ) AS interval_start,
    CASE WHEN date >= ${start_date} AND previous IS NOT NULL THEN STRUCT(
      previous.date AS date,
      previous.time_ms AS time_ms,
      previous.osd_odometer AS value
    ) ELSE NULL END AS prev_osd_odo,
    STRUCT(
      date,
      time_ms,
      osd_odometer AS value
    ) AS osd_odo
  FROM windowed_odometer
  WHERE date >= ${start_date} OR next IS NULL OR next.date >= ${start_date}
),

-- osdodometer fixes the date column since exploded_osdodometer can output dates
-- that don't match interval_start.  We also filter to make sure interval_starts
-- are within [start_date, end_date).  We also filter out any osd_odo values
-- and nullify any prev_osd_odo values generated >30d before the interval_start
-- in order to have parity with the activity details report.
osdodometer AS (
  SELECT
    org_id,
    device_id,
    CAST(interval_start as date) as date,
    interval_start,
    CASE WHEN prev_osd_odo.time_ms >= (unix_timestamp(interval_start)*1000) - 2592000000 THEN prev_osd_odo ELSE NULL END as prev_osd_odo,
    osd_odo
  FROM exploded_osdodometer
  WHERE interval_start >= ${start_date}
  AND interval_start < ${end_date}
  AND (osd_odo IS NULL OR osd_odo.time_ms >= (unix_timestamp(interval_start)*1000) - 2592000000)
),

-- osdderivedgpsdistance is a table with the same structure as osdodometer
-- above.  Like windowed_odometer, kinesisstats_window.osdgpsdistance is a table
-- with each row having it's current value, the previous value, and the next
-- value.  Here we're grabbing all values within [start_date, end_date).
osdderivedgpsdistance AS (
  SELECT
    org_id,
    object_id AS device_id,
    date,
    DATE_TRUNC('hour', FROM_UNIXTIME(value.time / 1000)) AS interval_start,
    STRUCT(
      value.time AS time_ms,
      value.double_value AS value
    ) AS osd_gps,
    STRUCT(
      COALESCE(previous.value.time, value.time) AS time_ms,
      COALESCE(previous.value.double_value, value.double_value) AS value
    ) AS prev_gps
  FROM kinesisstats_window.osdgpsdistance
  WHERE date >= ${start_date}
  AND date < ${end_date}
  AND value.time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000
),

-- canonical_distance is a table exaclty like total_distance except we convert
-- each time_ms to an 'interval_start'.  Here the distance value is just how far
-- the device has travelled since the last data point.  This table is high
-- granularity (datapoint every 5 seconds) so overcounting / undercounting at
-- the very begnning and end of each hour/'interval_start' is okay.
canonical_distance AS (
  SELECT
    org_id,
    device_id,
    date,
    DATE_TRUNC('hour', FROM_UNIXTIME(time_ms / 1000)) AS interval_start,
    distance,
    interpolated
  FROM canonical_distance.total_distance
  WHERE date >= ${start_date}
  AND date < ${end_date}
  AND time_ms <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000
),

-- exploded_trips is a table whose main purpose is to get the driver_id for each
-- 'interval_start'.  Since we only compute the interval the trip ended in for
-- the rest of the activity report, we need to add in all the other intervals
-- over the course of the trip.  We subtract 2 from the start_date to get trips
-- that started before start_date.  These trips can contain driver_id, etc.
exploded_trips AS (
  SELECT
    org_id,
    device_id,
    COALESCE(driver_id_exclude_0_drivers, 0) AS driver_id,
    EXPLODE(
      SEQUENCE(
        DATE_TRUNC('hour', FROM_UNIXTIME(proto.start.time / 1000)),
        DATE_TRUNC('hour', FROM_UNIXTIME(proto.end.time / 1000)),
        INTERVAL 1 HOUR
      )
    ) as interval_start
  FROM trips2db_shards.trips
  WHERE date >= DATE_SUB(${start_date}, 2) AND date < ${end_date}
  AND (proto.ongoing IS NULL OR NOT proto.ongoing)
  AND version = 101
  AND org_id NOT IN (1, 562949953421343)
),

-- trips is a table that just grabs the unique rows from exploded_trips (there
-- can be a lot of duplicate driver_id entries if the device makes a lot of tiny
-- trips within the same hour / 'interval_start') and adds a date column.
trips AS (
  SELECT DISTINCT *,
    DATE_FORMAT(interval_start, 'yyyy-MM-dd') AS date
  FROM exploded_trips
  WHERE DATE_FORMAT(interval_start, 'yyyy-MM-dd') >= ${start_date}
),

-- trips_with_gps is a table that full joins on `interval_start' between trips
-- and osdderivedgpsdistance
trips_with_gps AS (
  SELECT
    org_id,
    device_id,
    date,
    interval_start,
    COALESCE(MAX(driver_id), 0) AS driver_id,
    MIN(prev_gps).value AS min_gps,
    MAX(osd_gps).value AS max_gps
  FROM trips t
  FULL JOIN osdderivedgpsdistance og USING(org_id, date, device_id, interval_start)
  GROUP BY
    org_id,
    device_id,
    date,
    interval_start
),

-- trips_with_odo is a table that full joins on 'interval_start' between
-- trips_with_gps and osdodometer.  When selecting the min_odo, we want to
-- instead look at the previous values if those are not null.
trips_with_odo AS (
  SELECT
    org_id,
    device_id,
    date,
    interval_start,
    COALESCE(MAX(driver_id), 0) AS driver_id,
    MIN(min_gps) AS min_gps,
    MAX(max_gps) AS max_gps,
    MIN(COALESCE(prev_osd_odo, osd_odo)).value AS min_odo,
    MAX(osd_odo).value AS max_odo
  FROM trips_with_gps t
  FULL JOIN osdodometer od USING(org_id, date, device_id, interval_start)
  GROUP BY
    org_id,
    device_id,
    date,
    interval_start
),

-- trips_with_distance is a table that full joins on 'interval_start' between
-- trips_with_odo and canonical_distance.
trips_with_distance AS (
  SELECT
    org_id,
    device_id,
    date,
    COALESCE(MAX(driver_id), 0) AS driver_id,
    interval_start,
    MIN(min_gps) AS min_gps,
    MAX(max_gps) AS max_gps,
    MIN(min_odo) AS min_odo,
    MAX(max_odo) AS max_odo,
    SUM(distance) AS canonical_distance_m,
    MIN(interpolated) AS interpolated
  FROM trips_with_odo t
  FULL JOIN canonical_distance cd USING(org_id, date, device_id, interval_start)
  GROUP BY
    org_id,
    device_id,
    date,
    interval_start
),

-- Now let's use synthetic odometer for min_odo and max_odo for each
-- interval_start if min_odo or max_odo is null (i.e. prioritize OBD odometer
-- but fall back to synthetic odometer if the vehicle isn't reporting OBD
-- odometer)

-- manual_odometer is a table that structifies and creates a 'manual_odo' column
-- so that when we later do a MAX() it correctly grabs the latest updated manual
-- odometer first.  When you MAX() over a struct, it looks at each field in turn
-- to determine priority so if there's a tie for 'manual_odometer_updated_at' (which isn't possible)
manual_odometer AS (
  SELECT *,
    STRUCT(
      manual_odometer_updated_at,
      manual_odometer_meters,
      gps_distance_at_manual_update
    ) AS manual_odo
  FROM activity_report.manual_odometer_2
  WHERE date < ${end_date}
),

-- trips_with_manual_odometer is a table that left joins trips_with_distance
-- with manual_odometer.  Basically every 'interval_start' will have gps,
-- odometer, and distance values with the manual_odo struct (if it exists).
trips_with_manual_odometer AS (
  SELECT
    d.org_id,
    d.device_id,
    d.date,
    interval_start,
    MAX(driver_id) AS driver_id,
    MIN(min_odo) AS min_odo,
    MAX(max_odo) AS max_odo,
    MIN(min_gps) AS min_gps,
    MAX(max_gps) AS max_gps,
    MAX(canonical_distance_m) AS canonical_distance_m,
    MIN(interpolated) AS interpolated,
    MAX(manual_odo) AS manual_odometer
  FROM trips_with_distance d
  LEFT JOIN manual_odometer mo
  ON d.org_id = mo.org_id
  AND d.device_id = mo.device_id
  AND mo.date <= d.date
  GROUP BY
    d.org_id,
    d.device_id,
    d.date,
    interval_start
),

-- trips_with_synthetic_odometer is a table that pulls canonical_distance from
-- trips_with_manual_odometer and calculates gps_distance and start/end
-- odometers.  It will fall back to synthetic odometers if min_odo/max_odo
-- (which are OBD values) are null.
trips_with_synthetic_odometer as (
  SELECT
    org_id,
    date,
    device_id,
    driver_id,
    interval_start,
    canonical_distance_m,
    interpolated,
    max_gps - min_gps AS gps_distance_m,
    CAST(COALESCE(min_odo, min_gps - manual_odometer.gps_distance_at_manual_update + manual_odometer.manual_odometer_meters) AS LONG) AS start_odometer_m,
    CAST(COALESCE(max_odo, max_gps - manual_odometer.gps_distance_at_manual_update + manual_odometer.manual_odometer_meters) AS LONG) AS end_odometer_m
  FROM trips_with_manual_odometer
),

-- We'll come back to trips_with_synthetic_odometer further down. For now, we
-- need to grab engine_ms and number of stops for device trips and driver trips.

-- Again, subtracting 2 days here helps us get trips that start before
-- start_date but end in the range [start_date, end_date)
device_trips_bucket_end_time AS (
  SELECT
    org_id,
    device_id,
    from_unixtime(proto.end.time / 1000, "yyyy-MM-dd") as date,
    version,
    start_ms,
    date_trunc('hour', from_unixtime(proto.end.time / 1000)) AS interval_start,
    date_trunc('hour', from_unixtime(proto.end.time / 1000)) + interval 1 hour AS interval_end,
    STRUCT(
        proto.start.time AS start_ms,
        proto.end.time AS end_ms
    ) AS trip
  FROM trips2db_shards.trips AS t
  WHERE date >= date_add(${start_date}, -2) AND date < ${end_date}
  AND (t.proto.ongoing IS NULL OR NOT t.proto.ongoing)
  AND version = 101
  AND org_id NOT IN (1, 562949953421343)
),

device_engine_ms AS (
  SELECT *,
  CASE WHEN CAST(from_unixtime(trip.end_ms / 1000) AS TIMESTAMP) > interval_end
    THEN (CAST(to_unix_timestamp(interval_end) AS BIGINT) * 1000) - trip.start_ms
    ELSE trip.end_ms - trip.start_ms END
    AS engine_ms
  FROM device_trips_bucket_end_time
),

device_rows AS (
  SELECT
    org_id,
    device_id as object_id,
    date,
    interval_start,
    SUM(engine_ms) AS engine_ms,
    COUNT(*) AS stops_count
  FROM device_engine_ms
  GROUP BY org_id, device_id, date, interval_start
  HAVING date >= ${start_date}
),

driver_trips_bucket_end_time AS (
  SELECT
    org_id,
    from_unixtime(proto.end.time / 1000, "yyyy-MM-dd") as date,
    version,
    start_ms,
    driver_id_exclude_0_drivers as driver_id,
    date_trunc('hour', from_unixtime(proto.end.time / 1000)) AS interval_start,
    date_trunc('hour', from_unixtime(proto.end.time / 1000)) + interval 1 hour AS interval_end,
    STRUCT(
        proto.start.time AS start_ms,
        proto.end.time AS end_ms
    ) AS trip
  FROM trips2db_shards.trips AS t
  WHERE date >= date_add(${start_date}, -2) AND date < ${end_date}
  AND (t.proto.ongoing IS NULL OR NOT t.proto.ongoing)
  AND version = 101
  AND COALESCE(driver_id_exclude_0_drivers, 0) != 0
  AND org_id NOT IN (1, 562949953421343)
),

driver_engine_ms AS (
  SELECT *,
  CASE WHEN CAST(from_unixtime(trip.end_ms / 1000) AS TIMESTAMP) > interval_end
    THEN (CAST(to_unix_timestamp(interval_end) AS BIGINT) * 1000) - trip.start_ms
    ELSE trip.end_ms - trip.start_ms END
    AS engine_ms
  FROM driver_trips_bucket_end_time
),

driver_rows AS (
  SELECT
    org_id AS org_id,
    driver_id AS object_id,
    date,
    5 AS object_type,
    interval_start AS interval_start,
    SUM(engine_ms) AS engine_ms,
    COUNT(*) AS stops_count
  FROM driver_engine_ms
  GROUP BY org_id, driver_id, date, interval_start
  HAVING date >= ${start_date}
),

report_rows AS (
  SELECT
    org_id,
    object_id,
    date,
    1 AS object_type,
    interval_start,
    stops_count,
    engine_ms
  FROM device_rows
  WHERE date >= ${start_date}
  AND date < ${end_date}
  UNION ALL (
    SELECT
      org_id,
      object_id,
      date,
      5 AS object_type,
      interval_start,
      stops_count,
      engine_ms
    FROM driver_rows
    WHERE date >= ${start_date}
    AND date < ${end_date}
  )
),

trip_device_intervals AS (
  SELECT
    org_id,
    1 AS object_type,
    device_id AS object_id,
    date,
    interval_start,
    start_odometer_m,
    end_odometer_m,
    canonical_distance_m,
    interpolated,
    gps_distance_m
  FROM trips_with_synthetic_odometer
  WHERE date >= ${start_date}
  AND date < ${end_date}
),

trip_driver_intervals AS (
  SELECT
    org_id,
    5 AS object_type,
    driver_id AS object_id,
    date,
    interval_start,
    start_odometer_m,
    end_odometer_m,
    canonical_distance_m,
    interpolated,
    gps_distance_m
  FROM trips_with_synthetic_odometer
  WHERE driver_id != 0
  AND date >= ${start_date}
  AND date < ${end_date}
),

trip_intervals AS (
  SELECT * FROM trip_device_intervals
  UNION (
    SELECT * FROM trip_driver_intervals
  )
)

SELECT
  org_id,
  object_id,
  date,
  object_type,
  interval_start,
  SUM(gps_distance_m) AS gps_distance_m,
  SUM(canonical_distance_m) AS canonical_distance_m,
  MIN(interpolated) AS distance_interpolated,
  SUM(engine_ms) AS engine_ms,
  SUM(stops_count) AS stops_count,
  MIN(start_odometer_m) AS start_odometer_m,
  MAX(end_odometer_m) AS end_odometer_m
FROM trip_intervals
LEFT JOIN report_rows
USING(org_id, object_id, date, object_type, interval_start)
GROUP BY
  org_id,
  object_id,
  date,
  object_type,
  interval_start
