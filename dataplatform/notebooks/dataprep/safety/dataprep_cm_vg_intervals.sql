-- Databricks notebook source

-- Fetch trips for each device. We mark the on trip flag for each trip as true

CREATE OR REPLACE TEMP VIEW device_trip_intervals AS (
  SELECT
      trusted_cm_vgs.vg_device_id AS device_id,
      trusted_cm_vgs.linked_cm_id AS cm_device_id,
      trusted_cm_vgs.org_id,
      trusted_cm_vgs.org_name,
      trusted_cm_vgs.org_type,
      trusted_cm_vgs.product_id,
      trusted_cm_vgs.cm_product_id,
      trips.proto.start.time AS start_ms,
      trips.proto.end.time AS end_ms,
      (trips.proto.end.time - trips.proto.start.time) AS duration_ms,
      COALESCE(trips.proto.trip_distance.distance_meters, 0) AS distance_meters,
      trips.proto.start.latitude AS start_lat,
      trips.proto.start.longitude AS start_lon,
      trips.proto.start.place.city AS start_city,
      trips.proto.start.place.state AS start_state,
      trips.proto.end.latitude AS end_lat,
      trips.proto.end.longitude AS end_lon,
      trips.proto.end.place.city AS end_city,
      trips.proto.end.place.state AS end_state,
      TRUE AS on_trip,
      trips.date
    FROM dataprep_safety.cm_linked_vgs AS trusted_cm_vgs
    JOIN trips2db_shards.trips AS trips
      ON trusted_cm_vgs.org_id = trips.org_id
     AND trusted_cm_vgs.vg_device_id = trips.device_id
    WHERE trips.proto.start.time <> trips.proto.end.time
    AND (trips.date between
      COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),30)) and
      COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1)))
    AND trips.proto.end.time <= TO_UNIX_TIMESTAMP(CURRENT_DATE())*1000
    AND trips.version = 101

);

-- COMMAND ----------

-- Fetch all intervals where the device was not on a trip.
-- This is calculated from the prev_end.time field in the trips proto.
-- The interval is defined from the end time of the previous trip to the start time of the current trip.
-- The on_trip field is set to false

CREATE OR REPLACE TEMP VIEW device_non_trip_intervals AS (
  SELECT
    trusted_cm_vgs.vg_device_id AS device_id,
    trusted_cm_vgs.linked_cm_id AS cm_device_id,
    trusted_cm_vgs.org_id,
    trusted_cm_vgs.org_name,
    trusted_cm_vgs.org_type,
    trusted_cm_vgs.product_id,
    trusted_cm_vgs.cm_product_id,
    trips.proto.prev_end.time AS start_ms,
    trips.proto.start.time AS end_ms,
    (trips.proto.start.time - trips.proto.prev_end.time) AS duration_ms,
    NULL AS distance_meters,
    trips.proto.prev_end.latitude AS start_lat,
    trips.proto.prev_end.longitude AS start_lon,
    trips.proto.prev_end.place.city AS start_city,
    trips.proto.prev_end.place.state AS start_state,
    trips.proto.start.latitude AS end_lat,
    trips.proto.start.longitude AS end_lon,
    trips.proto.start.place.city AS end_city,
    trips.proto.start.place.state AS end_state,
    FALSE AS on_trip,
    trips.date
  FROM dataprep_safety.cm_linked_vgs AS trusted_cm_vgs
  JOIN trips2db_shards.trips AS trips
    ON trusted_cm_vgs.org_id = trips.org_id
    AND trusted_cm_vgs.vg_device_id = trips.device_id
  WHERE trips.proto.prev_end IS NOT NULL
    AND trips.proto.prev_end.time <> trips.proto.start.time
    AND (trips.date between
      COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),30)) and
      COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1)))
    AND trips.proto.end.time <= TO_UNIX_TIMESTAMP(CURRENT_DATE())*1000
    AND trips.version = 101
);

-- COMMAND ----------

-- Fetch the last heartbeat for each CM.
-- This will be used in calculating the most
-- recent non trip interval for each CM.

CREATE OR REPLACE TEMP VIEW cm_vgs_last_heartbeat AS (
  SELECT
        cm_linked_vgs.vg_device_id AS device_id,
        cm_linked_vgs.linked_cm_id,
        cm_linked_vgs.org_id,
        cm_linked_vgs.org_name,
        cm_linked_vgs.org_type,
        cm_linked_vgs.product_id,
        cm_linked_vgs.cm_product_id,
        device_heartbeats_extended.last_heartbeat_ms AS cm_last_heartbeat_ms
    FROM dataprep_safety.cm_linked_vgs AS cm_linked_vgs
    JOIN dataprep.device_heartbeats_extended AS device_heartbeats_extended
      ON cm_linked_vgs.linked_cm_id = device_heartbeats_extended.device_id
     AND cm_linked_vgs.org_id = device_heartbeats_extended.org_id
);

-- COMMAND ----------

-- Calculate the most recent non trip interval for each CM
-- We have to do this because we initially calculate the
-- non trip intervals by using the prev_end field in the trips proto
-- However, suppose a device went on its most recent trip two weeks ago.
-- We would not be able to capture the last two weeks for this device
-- There is no row in the trips column that has the most recent trip end ms in the prev_end field
-- So we manually construct the most recent non trip interval for each CM
-- The start ms is the end ms of the most recent trip the device went on
-- The end ms is the last heartbeat ms of the CM

CREATE OR REPLACE TEMP VIEW device_recent_non_trip_intervals AS (
  SELECT
    trusted_cm_vgs.device_id,
    trusted_cm_vgs.linked_cm_id AS cm_device_id,
    trusted_cm_vgs.org_id,
    trusted_cm_vgs.org_name,
    trusted_cm_vgs.org_type,
    trusted_cm_vgs.product_id,
    trusted_cm_vgs.cm_product_id,
    max(trips.proto.end.time) AS start_ms,
    trusted_cm_vgs.cm_last_heartbeat_ms AS end_ms,
    TO_UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 1))*1000 - MAX(trips.proto.end.time) AS duration_ms,
    NULL AS distance_meters,
    max((trips.proto.end.time, trips.proto.end.latitude)).latitude AS start_lat,
    max((trips.proto.end.time, trips.proto.end.longitude)).longitude AS start_lon,
    max((trips.proto.end.time, trips.proto.end.place.city)).city AS start_city,
    max((trips.proto.end.time, trips.proto.end.place.state)).state AS start_state,
    NULL AS end_lat,
    NULL AS end_lon,
    NULL AS end_city,
    NULL AS end_state,
    FALSE AS on_trip,
    MAX((trips.proto.end.time, trips.date)).date AS date
  FROM cm_vgs_last_heartbeat AS trusted_cm_vgs
  JOIN trips2db_shards.trips AS trips
    ON trusted_cm_vgs.org_id = trips.org_id
    AND trusted_cm_vgs.device_id = trips.device_id
 WHERE trips.proto.prev_end IS NOT NULL
   AND trips.proto.prev_end.time <> trips.proto.start.time
   AND (trips.date between
     COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),30)) and
     COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1)))
   AND trips.proto.end.time <= TO_UNIX_TIMESTAMP(CURRENT_DATE())*1000
   AND trips.version = 101
   AND trusted_cm_vgs.cm_last_heartbeat_ms >= trips.proto.end.time
 GROUP BY
    trusted_cm_vgs.device_id,
    trusted_cm_vgs.linked_cm_id,
    trusted_cm_vgs.org_id,
    trusted_cm_vgs.org_name,
    trusted_cm_vgs.org_type,
    trusted_cm_vgs.product_id,
    trusted_cm_vgs.cm_product_id,
    trusted_cm_vgs.cm_last_heartbeat_ms
);

-- COMMAND ----------

-- For non trip intervals between 2023-03-01 and 2023-03-08, duplicate trips.proto.prev_end.time values
-- are causing ambiguous merge issues. Telematics data has rolled back the change causing these issues,
-- however the fix will not be retroactively applied to erroneous entries from 2023-03-01 and 2023-03-08.
-- Update: As of 10 Mar, we are still seeing duplicates in the proto.prev_end.time field. Modify this
-- fix to apply for all date ranges.

CREATE OR REPLACE TEMP VIEW remove_duplicate_start_ms AS (
  SELECT device_id, cm_device_id, org_id, org_name, org_type, product_id, cm_product_id, start_ms,
    first(end_ms) AS end_ms,
    first(duration_ms) AS duration_ms,
    first(distance_meters) AS distance_meters,
    first(start_lat) AS start_lat,
    first(start_lon) AS start_lon,
    first(start_city) AS start_city,
    first(start_state) AS start_state,
    first(end_lat) AS end_lat,
    first(end_lon) AS end_lon,
    first(end_city) AS end_city,
    first(end_state) AS end_state,
    first(on_trip) AS on_trip,
    first(date) AS date
  FROM device_non_trip_intervals
  GROUP BY device_id, cm_device_id, org_id, org_name, org_type, product_id, cm_product_id, start_ms
  ORDER BY end_ms
);

-- COMMAND ----------

-- Union trip intervals, non trip intervals, & recent non trip intervals
-- This gives us all trip & non trip intervals for a device

CREATE OR REPLACE TEMP VIEW trips_union AS (
  SELECT * FROM device_trip_intervals
  UNION
  SELECT * FROM remove_duplicate_start_ms
  UNION
  SELECT * FROM device_recent_non_trip_intervals WHERE end_lat IS NOT NULL -- recent non-trip intervals w/ NULL end_lat/long causing merge issues
);

-- COMMAND ----------

-- Now we have to deal with intervals that span multiple days
-- We want to break these down into multiple intervals that are
-- split at the day boundary.

-- We start by converting the start_ms & end_ms columns for each
-- interval into dates

CREATE OR REPLACE TEMP VIEW trips_union_dates AS (
  SELECT
    date,
    device_id,
    cm_device_id,
    org_id,
    org_name,
    org_type,
    product_id,
    cm_product_id,
    start_ms,
    end_ms,
    from_utc_timestamp(to_timestamp(from_unixtime(start_ms/1000, 'yyyy-MM-dd HH:mm:ss')), 'UTC') AS start_ts,
    from_utc_timestamp(to_timestamp(from_unixtime(end_ms/1000, 'yyyy-MM-dd HH:mm:ss')), 'UTC') AS end_ts,
    duration_ms,
    distance_meters,
    start_lat,
    start_lon,
    start_city,
    start_state,
    end_lat,
    end_lon,
    end_city,
    end_state,
    on_trip
  FROM trips_union
);

-- COMMAND ----------

-- We also create a temp dates table. This just contains every
-- date from the first trip in the trips db to now
-- This will be used later in breaking intervals spanning multiple
-- days into individual intervals for each day

CREATE OR REPLACE TEMP VIEW dates AS (
  SELECT
    date
  FROM definitions.445_calendar
  WHERE date BETWEEN (SELECT MIN(date) FROM trips_union) AND CURRENT_DATE()
);

-- COMMAND ----------

-- Filter out intervals that do not span multiple days

CREATE OR REPLACE TEMP VIEW multi_day_trips AS (
  SELECT
    date,
    device_id,
    cm_device_id,
    org_id,
    org_name,
    org_type,
    product_id,
    cm_product_id,
    start_ms,
    end_ms,
    start_ts,
    end_ts,
    duration_ms,
    distance_meters,
    start_lat,
    start_lon,
    start_city,
    start_state,
    end_lat,
    end_lon,
    end_city,
    end_state,
    on_trip
  FROM trips_union_dates
  WHERE DATE(start_ts) != DATE(end_ts)
);

-- COMMAND ----------

-- Use the dates dataframe to get each date in between the start
-- and end date of multi day trip intervals

CREATE OR REPLACE TEMP VIEW multi_day_trip_intervals AS (
  SELECT
    tudf.date,
    tudf.device_id,
    tudf.cm_device_id,
    tudf.org_id,
    tudf.org_name,
    tudf.org_type,
    tudf.product_id,
    tudf.cm_product_id,
    tudf.start_ms,
    tudf.end_ms,
    tudf.start_ts,
    tudf.end_ts,
    tudf.duration_ms,
    tudf.distance_meters,
    tudf.start_lat,
    tudf.start_lon,
    tudf.start_city,
    tudf.start_state,
    tudf.end_lat,
    tudf.end_lon,
    tudf.end_city,
    tudf.end_state,
    tudf.on_trip,
    d.date AS interval_date
  FROM multi_day_trips AS tudf
  LEFT JOIN dates AS d
  ON d.date BETWEEN DATE(tudf.start_ts) AND DATE(tudf.end_ts)
);

-- COMMAND ----------

-- We now have a row for each date spanned by a multi date interval
-- What we want however is a start ms and end ms for each of those dates
-- So we take each date and calculate the beginning of the day as the start ms
-- For the end ms we add 24 hours and subtract 1 ms to get the end ms (11:59:59:999)
-- For the actual start date and end date of the trip we don't want to use
-- the beginning of the day & end of the day as the start&end ms, we want to use the interval's
-- actual start & end ms, so if the date equals the interval start date we use the interval start ms otherwise we use beginning of the day start ms
-- Similarly if the date is the interval end date we use the interval end ms otherwise we use the end of day end ms

CREATE OR REPLACE TEMP VIEW device_intervals_multi_day_new_ms AS (
  SELECT
    date,
    device_id,
    cm_device_id,
    org_id,
    org_name,
    org_type,
    product_id,
    cm_product_id,
    start_ms,
    end_ms,
    start_ts,
    end_ts,
    duration_ms,
    distance_meters,
    start_lat,
    start_lon,
    start_city,
    start_state,
    end_lat,
    end_lon,
    end_city,
    end_state,
    on_trip,
    IF(DATE(start_ts) = interval_date, start_ms, (to_unix_timestamp(interval_date, 'yyyy-MM-dd') * 1000)) AS start_ms_new,
    IF(DATE(end_ts) = interval_date, end_ms, (to_unix_timestamp(interval_date, 'yyyy-MM-dd') * 1000 + 24 * 60 * 60 * 1000 - 1)) AS end_ms_new
  FROM  multi_day_trip_intervals
);

-- COMMAND ----------

-- Group by common dimensions and new start_ms and end_ms.
-- This grouping is necessary due to a bug introduced into trips2db_shards.trips, where some non-trip intervals
-- can have overlapping proto.prev_end.time <> proto.start.time intervals.
-- This results in certain non-trip intervals creating multiple entries for a given date (in the previous view),
-- based on different non-trip start_ms.

CREATE OR REPLACE TEMP VIEW device_intervals_multi_day_remove_dupes AS (
  SELECT
    MAX_BY(date, start_ms) AS date,
    device_id,
    cm_device_id,
    org_id,
    org_name,
    org_type,
    product_id,
    cm_product_id,
    MAX_BY(start_ms, start_ms) AS start_ms,
    MAX_BY(end_ms, start_ms) AS end_ms,
    MAX_BY(start_ts, start_ms) AS start_ts,
    MAX_BY(end_ts, start_ms) AS end_ts,
    MAX_BY(duration_ms, start_ms) AS duration_ms,
    MAX_BY(distance_meters, start_ms) AS distance_meters,
    MAX_BY(start_lat, start_ms) AS start_lat,
    MAX_BY(start_lon, start_ms) AS start_lon,
    MAX_BY(start_city, start_ms) AS start_city,
    MAX_BY(start_state, start_ms) AS start_state,
    MAX_BY(end_lat, start_ms) AS end_lat,
    MAX_BY(end_lon, start_ms) AS end_lon,
    MAX_BY(end_city, start_ms) AS end_city,
    MAX_BY(end_state, start_ms) AS end_state,
    MAX_BY(on_trip, start_ms) AS on_trip,
    start_ms_new,
    end_ms_new
  FROM device_intervals_multi_day_new_ms
  -- GROUP BY columns that will uniquely identify a device for a particular interval
  GROUP BY device_id, cm_device_id, org_id, org_name, org_type, product_id, cm_product_id,
    start_ms_new, end_ms_new
);

-- COMMAND ----------

-- Rename old start_ms & end_ms, and recalculate duration_ms

CREATE OR REPLACE TEMP VIEW device_intervals_multi_day_recalc AS (
  SELECT
    CAST(date(from_unixtime(start_ms_new/1000)) AS STRING) AS date, -- Casting date as string explicitly to prevent major slowdown of downstream jobs
    device_id,
    cm_device_id,
    org_id,
    org_name,
    org_type,
    product_id,
    cm_product_id,
    DATE(start_ts) AS start_date,
    DATE(end_ts) AS end_date,
    start_ms_new AS start_ms,
    end_ms_new AS end_ms,
    (end_ms_new - start_ms_new) AS duration_ms,
    distance_meters,
    start_lat,
    start_lon,
    start_city,
    start_state,
    end_lat,
    end_lon,
    end_city,
    end_state,
    on_trip
  FROM  device_intervals_multi_day_remove_dupes
);

-- COMMAND ----------

-- Intervals that did not span multiple dates

CREATE OR REPLACE TEMP VIEW non_multi_day_trips AS (
  SELECT
    CAST(date(from_unixtime(start_ms/1000)) AS STRING) AS date, -- Casting date as string explicitly to prevent major slowdown of downstream jobs
    device_id,
    cm_device_id,
    org_id,
    org_name,
    org_type,
    product_id,
    cm_product_id,
    DATE(start_ts) AS start_date,
    DATE(end_ts) AS end_date,
    start_ms,
    end_ms,
    duration_ms,
    distance_meters,
    start_lat,
    start_lon,
    start_city,
    start_state,
    end_lat,
    end_lon,
    end_city,
    end_state,
    on_trip
  FROM trips_union_dates
  WHERE DATE(start_ts) = DATE(end_ts)
);

-- COMMAND ----------

-- Union broken up multi date trips with non-multi date trips
-- We now have set of all intervals for each device, but
-- no interval spans more than one day

CREATE OR REPLACE TEMP VIEW device_intervals_day_break AS (
  SELECT * FROM device_intervals_multi_day_recalc where duration_ms > 0 -- 0 time trips not needed and causing merge issues
  UNION -- ALL
  SELECT * FROM non_multi_day_trips where duration_ms > 0 -- 0 time trips not needed and causing merge issues
);

-- COMMAND ----------

-- Create table. This was previously an insert overwrite
-- so using preferred REPLACE pattern instead for this update.

CREATE TABLE IF NOT EXISTS dataprep_safety.cm_vg_intervals
USING DELTA
PARTITIONED BY (date) AS
SELECT * FROM device_intervals_day_break

-- COMMAND ----------

create or replace temporary view cm_vg_intervals_updates as (
  select DISTINCT * FROM device_intervals_day_break
  WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),30))
                 and COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1))
);

-- COMMAND ----------

merge into dataprep_safety.cm_vg_intervals as target
using cm_vg_intervals_updates as updates
on target.date = updates.date
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.org_id = updates.org_id
and target.start_ms = updates.start_ms
and target.end_ms = updates.end_ms -- edge case when two intervals have equal start_ms but different end_ms
when matched then update set *
when not matched then insert * ;
