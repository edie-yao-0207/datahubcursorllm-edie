WITH connected_interval_start_times AS (
  SELECT
    time,
    date,
    value,
    org_id,
    object_id AS vg_id,
    -- connected when int_value is 1
    CASE
      WHEN value.int_value = 1 THEN true
      ELSE false
    END AS is_connected
  FROM
    kinesisstats.osdrearcameraconnected
  WHERE
    -- include any time if connected within the last month
    date >= date_sub(${end_date}, 30)
    AND date < ${end_date}
    AND time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND value.is_databreak = false
    AND value.is_end = false
),
-- Determine which VGs are linked to CM3xs
vgs_with_cm3xs AS (
  SELECT
    devices.id AS vg_id,
    gateways.device_id AS cm_id,
    camera_first_connected_at_ms
  FROM
    productsdb.gateways gateways
    JOIN productsdb.devices devices ON upper(replace(gateways.serial, '-', '')) = upper(replace(devices.camera_serial, '-', ''))
    AND devices.org_id = gateways.org_id
  WHERE
    gateways.product_id IN (
      43,  -- CM32
      44,  -- CM31
      155, -- CM34 BrigidDual-256GB
      167  -- CM33 BrigidSingle
    )
),
-- VGs in enabled Baxter orgs that have sent up at least one connected object stat
vgs_with_camera_connector AS (
  SELECT
    org_id,
    connected.vg_id,
    cm_id,
    camera_first_connected_at_ms
  FROM
    connected_interval_start_times AS connected
    INNER JOIN vgs_with_cm3xs ON connected.vg_id = vgs_with_cm3xs.vg_id
  WHERE
    -- Device must have sent up at least one "connected" status
    is_connected = true
),
-- The first time a VG sends a connected object stat
first_connection_per_vg AS (
  SELECT
    min(time) AS first_connected_ms,
    vg_id
  FROM
    connected_interval_start_times
  WHERE
    is_connected = true
  GROUP BY
    vg_id
),
-- Trips that occured after the first time Baxter connected
trips AS (
  SELECT
    trips.org_id,
    device_id AS vg_id,
    cm_id,
    greatest(
      start_ms,
      unix_timestamp(to_timestamp(${start_date})) * 1000
    ) AS trip_start_ms,
    least(
      end_ms,
      unix_timestamp(to_timestamp(${end_date})) * 1000
    ) AS trip_end_ms,
    first_connection_per_vg.first_connected_ms
  FROM
    trips2db_shards.trips
    INNER JOIN first_connection_per_vg ON device_id = first_connection_per_vg.vg_id
    INNER JOIN vgs_with_camera_connector ON device_id = vgs_with_camera_connector.vg_id
  WHERE
    start_ms != end_ms
    AND date >= ${start_date}
    AND date < ${end_date} -- Exclude data in the past 4 hours due to ingestion delays.
    AND end_ms <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND proto.ongoing = false
    AND end_ms >= first_connection_per_vg.first_connected_ms
    and start_ms >= camera_first_connected_at_ms
    AND version = 101
),
-- All camera connected intervals with previous and next connected values
-- Used to combine stats where the connected state has not changed
previous_connected_states AS (
  SELECT
    org_id,
    vg_id,
    date,
    -- Add previous stat so we know the start of the range and if it was connected
    lag(
      time
    ) OVER (
      partition BY vg_id
      ORDER BY
        time ASC
    ) AS prev_time,
    lag(is_connected) OVER (
      partition BY vg_id
      ORDER BY
        time ASC
    ) AS prev_is_connected,
    -- Use current is_connected to filter for changes in connected
    time AS cur_time,
    is_connected AS cur_is_connected
  FROM
    connected_interval_start_times
),
-- Filter to state transitions or edges (null)
connected_state_transitions AS (
  SELECT
    *
  FROM
    previous_connected_states
  WHERE
    prev_is_connected != cur_is_connected
    OR prev_is_connected IS NULL
),
-- Looking forward to the start of the next transition
connected_state_transitions_with_next AS (
  SELECT
    org_id,
    vg_id,
    date,
    prev_time,
    COALESCE(prev_is_connected, cur_is_connected) AS prev_is_connected,
    CASE
      WHEN prev_time IS NULL THEN unix_timestamp(to_timestamp(${start_date})) * 1000
      ELSE cur_time
    END AS cur_time,
    cur_is_connected,
    COALESCE(
      lead(cur_time) OVER (
        partition BY vg_id
        ORDER BY
          cur_time ASC
      ),
      unix_timestamp(to_timestamp(${end_date})) * 1000
    ) AS next_time,
    lead(cur_is_connected) OVER (
      partition BY vg_id
      ORDER BY
        cur_time ASC
    ) AS next_is_connected
  FROM
    connected_state_transitions
),
-- Connected intervals, on or off trips
connected_intervals AS (
  SELECT
    org_id,
    vg_id,
    date,
    cur_time AS start_ms,
    next_time AS end_ms
  FROM
    connected_state_transitions_with_next
  WHERE
    cur_is_connected = true
),
-- Connected intervals that overlap trips
connected_intervals_overlapping_trips AS (
  SELECT
    trips.org_id,
    trips.vg_id,
    trips.cm_id,
    trip_start_ms,
    trip_end_ms,
    connected.start_ms,
    connected.end_ms
  FROM
    trips
    LEFT JOIN connected_intervals AS connected ON trips.vg_id = connected.vg_id
    AND trips.org_id = connected.org_id
    AND NOT (
      trip_start_ms >= connected.end_ms
      OR trip_end_ms <= connected.start_ms
    )
  GROUP BY
    trips.org_id,
    trips.vg_id,
    trips.cm_id,
    trip_start_ms,
    trip_end_ms,
    connected.start_ms,
    connected.end_ms
),
-- connected intervals bounded by trip bounds
connected_intervals_on_trips AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    trip_start_ms,
    (trip_end_ms - trip_start_ms) AS trip_duration_ms,
    least(trip_end_ms, end_ms) AS connected_end_ms,
    greatest(trip_start_ms, start_ms) AS connected_start_ms,
    CASE
      WHEN end_ms IS NOT NULL
      AND start_ms IS NOT NULL THEN least(trip_end_ms, end_ms) - greatest(trip_start_ms, connected.start_ms)
      ELSE 0
    END AS connected_interval_ms
  FROM
    connected_intervals_overlapping_trips AS connected
),
-- recording intervals that overlap connected intervals
recording_intervals_overlapping_connected AS (
  SELECT
    connected.org_id,
    connected.vg_id,
    connected.cm_id,
    connected_interval_ms,
    connected_start_ms,
    connected_end_ms,
    trip_start_ms,
    trip_duration_ms,
    recording.start_ms,
    recording.end_ms
  FROM
    connected_intervals_on_trips AS connected
    LEFT JOIN camera_connector_health.cc_recording_intervals AS recording
    ON connected.vg_id = recording.vg_id
      AND recording.end_ms-recording.start_ms >= 1000
      AND NOT (
        connected_start_ms >= recording.end_ms
        OR connected_end_ms <= recording.start_ms
      )
  GROUP BY
    connected.org_id,
    connected.vg_id,
    connected.cm_id,
    trip_start_ms,
    connected_interval_ms,
    connected_start_ms,
    connected_end_ms,
    trip_duration_ms,
    recording.start_ms,
    recording.end_ms
),
-- recording intervals that overlap connected intervals
recording_intervals_on_connected_on_trips AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    trip_start_ms,
    trip_duration_ms,
    connected_start_ms,
    connected_interval_ms,
    CASE
      WHEN end_ms IS NOT NULL
      AND start_ms IS NOT NULL THEN least(connected_end_ms, end_ms) - greatest(connected_start_ms, recording.start_ms)
      ELSE 0
    END AS recording_interval_ms
  FROM
    recording_intervals_overlapping_connected AS recording
),
-- total recording duration per connected interval
recording_intervals_per_connected_interval AS (
  SELECT
    org_id,
    vg_id,
    cm_id,
    connected_start_ms,
    connected_interval_ms,
    trip_start_ms,
    trip_duration_ms,
    least(
      sum(recording_interval_ms),
      connected_interval_ms
    ) AS recording_per_connected_ms
  FROM
    recording_intervals_on_connected_on_trips
  GROUP BY
    org_id,
    vg_id,
    cm_id,
    connected_start_ms,
    connected_interval_ms,
    trip_start_ms,
    trip_duration_ms
) -- Sum total time recording and connected durations per trip
SELECT
  org_id,
  vg_id,
  cm_id,
  trip_start_ms,
  date_format(
    from_unixtime(trip_start_ms / 1000),
    'yyyy-MM-dd'
  ) AS date,
  trip_duration_ms,
  COALESCE(sum(connected_interval_ms), 0) AS connected_ms,
  COALESCE(sum(recording_per_connected_ms), 0) AS recording_ms
FROM
  recording_intervals_per_connected_interval
GROUP BY
  org_id,
  vg_id,
  cm_id,
  trip_start_ms,
  trip_duration_ms
ORDER BY
  trip_start_ms ASC
