-- Auxcams that are currently associated with a VG
with collections_with_auxcams as (
  select
    device_id as auxcam_id,
    collection_uuid
  from
    deviceassociationsdb_shards.device_collection_associations
  where
    product_id = 126 -- Octo
    and end_at is null
),
-- Devices that are currently associated with an auxcam
non_auxcams_in_auxcam_collection as (
  select
    device_id,
    collection_uuid
  from
    deviceassociationsdb_shards.device_collection_associations
  where
    collection_uuid in (
      select
        collection_uuid
      from
        collections_with_auxcams
    )
    and product_id != 126 -- Octo
    and end_at is null
),
-- Auxcams and the devices in the same conllection
auxcams_and_associated_devices as (
  select
    device_id,
    auxcam_id
  from
    non_auxcams_in_auxcam_collection n
    inner join collections_with_auxcams c on n.collection_uuid = c.collection_uuid
  group by
    device_id,
    auxcam_id
),
-- VGs sending up multicam connected states with multicam type of NVR10, sent only with Auxcam connection
-- NOTE: after we have fully transitioned from osdmulticamconnected to osddashcamconnected, we can remove
-- this query to multicamconnected object stats.
multicam_connected_interval_start_times AS (
  SELECT
    time,
    date,
    org_id,
    auxcam_id,
    object_id AS vg_id,
    -- connected when int_value is 1
    CASE
      WHEN value.int_value = 1 THEN true
      ELSE false
    END AS is_connected,
    false as dashcam
  FROM
    kinesisstats.osdmulticamconnected ks
    inner join auxcams_and_associated_devices a on ks.object_id = a.device_id
  WHERE
    -- include any time if connected within the last month
    date >= date_sub(${end_date}, 30)
    AND date <= ${end_date}
    AND time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND value.is_databreak = false
    AND value.is_end = false
    AND value.proto_value.multicam_connection_state.multicam_type IN (1, 2, 3)
),
-- VGs sending up dashcam connected states with AHD1 or AHD4, sent only with Auxcam connection
dashcam_connected_interval_start_times AS (
  SELECT
    time,
    date,
    org_id,
    auxcam_id,
    object_id AS vg_id,
    -- connected when int_value is 1
    CASE
      WHEN value.int_value = 1 THEN true
      ELSE false
    END AS is_connected,
    true as dashcam
  FROM
    kinesisstats.osddashcamconnected ks
    inner join auxcams_and_associated_devices a on ks.object_id = a.device_id
  WHERE
    -- include any time if connected within the last month
    date >= date_sub(${end_date}, 30)
    AND date < ${end_date}
    AND time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND value.is_databreak = false
    AND value.is_end = false
    AND (
      value.proto_value.dashcam_connection_state.camera_product_type = 7
      OR value.proto_value.dashcam_connection_state.camera_product_type = 8
    )
),
-- combine multicam and dashcam connected object stats for cross-compatibility
combined_interval_start_times AS (
  SELECT
    *
  FROM
    multicam_connected_interval_start_times
  UNION
  SELECT
    *
  FROM
    dashcam_connected_interval_start_times
  ORDER BY
    vg_id,
    time asc
),
-- The first time a VG sends a connected object stat
first_connection_per_vg AS (
  SELECT
    min(time) AS first_connected_ms,
    vg_id
  FROM
    combined_interval_start_times
  WHERE
    is_connected = true
  GROUP BY
    vg_id
),
-- All auxcam connected intervals with previous and next connected values
-- Used to combine stats where the connected state has not changed
previous_connected_states AS (
  SELECT
    org_id,
    vg_id,
    date,
    -- Add previous stat so we know the start of the range and if it was connected
    lag(time) OVER (
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
    combined_interval_start_times
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
-- Trips that occured after the first time auxcam connected
trips AS (
  SELECT
    trips.org_id,
    d.device_id AS vg_id,
    auxcam_id,
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
    INNER JOIN auxcams_and_associated_devices d on trips.device_id=d.device_id
  WHERE
    start_ms != end_ms
    AND date >= ${start_date}
    AND date < ${end_date} -- Exclude data in the past 4 hours due to ingestion delays.
    AND end_ms <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND proto.ongoing = false
    AND end_ms >= first_connection_per_vg.first_connected_ms
    AND (version = 101 OR version = 201)
),
-- Connected intervals that overlap trips
connected_intervals_overlapping_trips AS (
  SELECT
    trips.org_id,
    trips.vg_id,
    trips.auxcam_id,
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
    trips.auxcam_id,
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
    auxcam_id,
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
-- only include recording intervals of 1 second or more, from the month before
recording_intervals as (
  SELECT
    *
  FROM camera_connector_health.auxcam_recording_intervals
  WHERE
    end_ms - start_ms >= 1000 -- MEDXP-25 filter out small intervals to avoid bad data
    AND date >= date_sub(${start_date}, 30)
),
-- Find the lowest start_ms of any overlapping recording intervals
recording_start_times AS (
  SELECT
    DISTINCT org_id,
    auxcam_id,
    start_ms
  FROM
    recording_intervals a
  WHERE
    NOT EXISTS(
      SELECT
        -- bucket timestamps into 1 hour chunks for improved join performance.
        /*+ RANGE_JOIN(b, 3600000) */
        *
      FROM
        recording_intervals b
      WHERE
        a.org_id = b.org_id
        AND a.auxcam_id = b.auxcam_id
        AND b.start_ms < a.start_ms
        AND b.end_ms >= a.start_ms
    )
),
-- Find the highest end_ms of any overlapping recording intervals
recording_end_times AS (
  SELECT
    DISTINCT org_id,
    auxcam_id,
    end_ms
  FROM
    recording_intervals a
  WHERE
    NOT EXISTS(
      SELECT
        -- bucket timestamps into 1 hour chunks for improved join performance.
        /*+ RANGE_JOIN(b, 3600000) */
        *
      FROM
        recording_intervals b
      WHERE
        a.org_id = b.org_id
        AND a.auxcam_id = b.auxcam_id
        AND b.end_ms > a.end_ms
        AND b.start_ms <= a.end_ms
    )
),
-- Correspond each start_ms with the closest end_ms after it
merged_recording_intervals AS (
  SELECT
    a.org_id,
    a.auxcam_id,
    a.start_ms,
    b.end_ms
  FROM
    recording_start_times AS a
    INNER JOIN recording_end_times b ON a.org_id = b.org_id
    AND a.auxcam_id = b.auxcam_id
    AND end_ms > start_ms
  WHERE
    NOT EXISTS(
      SELECT
        *
      FROM
        recording_end_times c
      WHERE
        a.org_id = c.org_id
        AND a.auxcam_id = c.auxcam_id
        AND a.start_ms < c.end_ms
        AND c.end_ms < b.end_ms
    )
),
-- recording intervals that overlap connected intervals
recording_intervals_overlapping_connected AS (
  SELECT
    connected.org_id,
    connected.vg_id,
    connected.auxcam_id,
    connected_interval_ms,
    connected_start_ms,
    connected_end_ms,
    trip_start_ms,
    trip_duration_ms,
    recording.start_ms,
    recording.end_ms
  FROM
    connected_intervals_on_trips AS connected
    LEFT JOIN merged_recording_intervals AS recording ON connected.auxcam_id = recording.auxcam_id
    AND NOT (
      connected_start_ms >= recording.end_ms
      OR connected_end_ms <= recording.start_ms
    )
  GROUP BY
    connected.org_id,
    connected.vg_id,
    connected.auxcam_id,
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
    auxcam_id,
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
    auxcam_id,
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
    auxcam_id,
    connected_start_ms,
    connected_interval_ms,
    trip_start_ms,
    trip_duration_ms
) -- Sum total time recording and connected durations per trip
SELECT
  org_id,
  vg_id,
  auxcam_id,
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
  auxcam_id,
  trip_start_ms,
  trip_duration_ms
ORDER BY
  trip_start_ms ASC
