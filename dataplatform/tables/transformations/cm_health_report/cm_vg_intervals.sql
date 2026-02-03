-- Hardcode hour buckets (in ms) for a 24 hour period. This will enable us to split trip
-- durations on the hour. Trips will be joined on this table to be split into <=1hr long
-- with subtrips with start_ms, end_ms, and duration_ms adjusted accordingly.
WITH split_times_ms AS (
  VALUES
    (0, 3600000),
    (3600000, 7200000),
    (7200000, 10800000),
    (10800000, 14400000),
    (14400000, 18000000),
    (18000000, 21600000),
    (21600000, 25200000),
    (25200000, 28800000),
    (28800000, 32400000),
    (32400000, 36000000),
    (36000000, 39600000),
    (39600000, 43200000),
    (43200000, 46800000),
    (46800000, 50400000),
    (50400000, 54000000),
    (54000000, 57600000),
    (57600000, 61200000),
    (61200000, 64800000),
    (64800000, 68400000),
    (68400000, 72000000),
    (72000000, 75600000),
    (75600000, 79200000),
    (79200000, 82800000),
    (82800000, 86400000) AS (split_start_ms, split_end_ms)
),
filtered_trips AS (
  SELECT
    *
  FROM
    trips2db_shards.trips
  WHERE
    trips.proto.start.time <> trips.proto.end.time
    AND -- we look at trips started within past 30 days
    trips.date >= ${start_date}
    AND -- it is possible for end_date to be behind current_date, so we must also upper bound
    trips.date < ${end_date}
    -- Exclude data in the past 4 hours due to ingestion delays.
    AND trips.proto.end.time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    -- Only include trips that have completed
    AND trips.proto.ongoing = false
    AND (trips.version = 101 OR trips.version = 201)
),
device_trip_intervals AS (
  SELECT
    trusted_cm_vgs.org_id,
    trusted_cm_vgs.vg_device_id AS device_id,
    trusted_cm_vgs.product_id,
    trusted_cm_vgs.linked_cm_id AS cm_device_id,
    trusted_cm_vgs.cm_product_id,
    trips.proto.start.time AS start_ms,
    trips.proto.end.time AS end_ms,
    (trips.proto.end.time - trips.proto.start.time) AS duration_ms,
    -- Treat trips longer than 24 hours as outliers
    CASE
      WHEN trips.proto.end IS NOT NULL
    AND (trips.proto.end.time - trips.proto.start.time) < 24 * 60 * 60 * 1000 then true
    ELSE false
    END AS on_trip,
    trips.date
  FROM
    cm_health_report.cm_2x_3x_linked_vgs AS trusted_cm_vgs
    JOIN filtered_trips AS trips ON trusted_cm_vgs.org_id = trips.org_id
    AND trusted_cm_vgs.vg_device_id = trips.device_id
    -- Only include trips after the camera was first connected. This ensures that if a camera was connected
    -- after a vg was installed, we don't count trips before the CM was connected as recording downtime.
    WHERE trips.proto.start.time >= trusted_cm_vgs.camera_first_connected_at_ms
),
-- Fetch all intervals where the device was not on a trip.
-- This is calculated from the prev_end.time field in the trips proto.
-- The interval is defined from the end time of the previous trip to the start time of the current trip.
-- The on_trip field is set to false
device_non_trip_intervals AS (
  SELECT
    trusted_cm_vgs.org_id,
    trusted_cm_vgs.vg_device_id AS device_id,
    trusted_cm_vgs.product_id,
    trusted_cm_vgs.linked_cm_id AS cm_device_id,
    trusted_cm_vgs.cm_product_id,
    trips.proto.prev_end.time AS start_ms,
    trips.proto.start.time AS end_ms,
    (
      trips.proto.start.time - trips.proto.prev_end.time
    ) AS duration_ms,
    false AS on_trip,
    trips.date
  FROM
    cm_health_report.cm_2x_3x_linked_vgs AS trusted_cm_vgs
    JOIN filtered_trips AS trips ON trusted_cm_vgs.org_id = trips.org_id
    AND trusted_cm_vgs.vg_device_id = trips.device_id
  WHERE
    trips.proto.prev_end IS NOT NULL
    AND trips.proto.prev_end.time <> trips.proto.start.time
),
split_trip_intervals AS (
  SELECT
    org_id,
    device_id,
    product_id,
    cm_device_id,
    cm_product_id,
    intervals.start_ms + t.split_start_ms AS start_ms,
    intervals.start_ms + least(t.split_end_ms, intervals.duration_ms) AS end_ms,
    least(
      --(1hr, remainder of last segment)
      t.split_end_ms - t.split_start_ms,
      greatest(0, intervals.duration_ms - t.split_start_ms)
    ) AS duration_ms,
    on_trip,
    intervals.duration_ms AS original_duration_ms,
    -- Write all split trips with the date of the original interval, even
    -- if they cross midnight. This is because the original unsplit trip
    -- is written to the date corresponding to interval start. In order
    -- for split trips to fall under the consistently correct date partition,
    -- they must be written to the same partition as the source data.
    from_unixtime(
      intervals.start_ms / 1000,
      "yyyy-MM-dd"
    ) AS date,
    t.split_start_ms,
    t.split_end_ms
  FROM
    device_trip_intervals intervals
    JOIN split_times_ms t ON intervals.duration_ms > t.split_start_ms
  WHERE
    on_trip = true
)
-- Union trip intervals, non trip intervals, & recent non trip intervals
-- This gives us all trip & non trip intervals for a device
SELECT
  org_id,
  device_id,
  product_id,
  cm_device_id,
  cm_product_id,
  start_ms,
  end_ms,
  duration_ms,
  date,
  on_trip
FROM
  device_trip_intervals
WHERE
  on_trip = false
UNION
SELECT
  org_id,
  device_id,
  product_id,
  cm_device_id,
  cm_product_id,
  start_ms,
  end_ms,
  duration_ms,
  date,
  on_trip
FROM
  split_trip_intervals
