-- All the VGs with rear camera reversing stat available
WITH osdvehiclecurrentgear AS (
  SELECT
    time,
    value,
    org_id,
    object_id AS device_id
  FROM
    kinesisstats.osdvehiclecurrentgear
  WHERE
  where
    date >= ${start_date}
    AND date < ${end_date}
    AND time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND value.is_databreak = false
    AND value.is_end = false
),
-- All the VGs with cables connnected
osdobdcableid AS (
  SELECT
    time,
    value,
    org_id,
    object_id AS vg_id
  FROM
    kinesisstats.osdobdcableid
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
    AND time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND value.is_databreak = false
    AND value.is_end = false
),
-- All the VGs with cable port types
osdcanbustype AS (
  SELECT
    time,
    value,
    org_id,
    object_id AS vg_id
  FROM
    kinesisstats.osdcanbustype
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
    AND time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND value.is_databreak = false
    AND value.is_end = false
),
-- All the VGs that have a possible J1939 cable
vgs_with_possible_j1939_cables AS (
  SELECT
    time,
    value.int_value,
    vg_id
  FROM
    osdobdcableid
  WHERE
    -- selecting for connection with potential to be J1939 cable, based on https://github.com/samsara-dev/backend/blob/master/ts/samsara-common/utils/constants.ts
    value.int_value IN (
      1,
      2,
      3,
      5,
      7,
      10
    )
),
-- All the VGs that have a J1939-compatible port
vgs_with_1939_ports AS (
  SELECT
    time,
    value.int_value,
    vg_id
  FROM
    osdcanbustype
  WHERE
    -- vehicles with J1939 ports, based on https://github.com/samsara-dev/backend/blob/06d7bd07ccf93731baa8bf967d8d3e2dfa557223/go/src/samsaradev.io/gqlschema/gqlecodriving/can_bus_support.go#L14-L29
    value.proto_value.can_bus_info.can_bus_type_enum IN (
      1,
      12,
      26,
      27,
      28,
      31,
      32,
      33,
      35,
      36,
      37,
      38,
      39,
      40
    )
),
-- Devices with reverse-only attributes
reverse_only_attribute_id_by_org_id AS (
  SELECT
    org_id,
    uuid
  FROM
    attributedb_shards.attribute_values
  WHERE
    string_value IN ("Reverse Only")
),
-- Reverse-only VGs with J1939-compatible cables and ports
reverse_only_vgs_with_j1939_cables AS (
  SELECT
    entity_id
  FROM
    attributedb_shards.attribute_cloud_entities
  WHERE
    attribute_value_id IN (
      SELECT
        uuid
      FROM
        reverse_only_attribute_id_by_org_id
    )
    AND entity_id IN (
      SELECT
        vg_id
      FROM
        vgs_with_possible_j1939_cables
    )
    AND entity_id IN (
      SELECT
        vg_id
      FROM
        vgs_with_1939_ports
    )
),
-- Reversing intervals start times
reversing_interval_start_times AS (
  SELECT
    time,
    value.int_value,
    org_id,
    CASE
      WHEN value.int_value < 0 THEN true
      ELSE false
    END AS is_reversing,
    device_id AS vg_id
  FROM
    osdvehiclecurrentgear
  WHERE
    device_id IN (
      SELECT
        *
      FROM
        reverse_only_vgs_with_j1939_cables
    )
  ORDER BY
    vg_id,
    time ASC
),
-- All reversing intervals with previous and next reversing values
-- Used to combine stats where the reversing state has not changed
previous_reversing_states AS (
  SELECT
    org_id,
    vg_id,
    -- Add previous stat so we know the start of the range and if it was reversing
    lag(
      time
    ) OVER (
      partition BY vg_id
      ORDER BY
        time ASC
    ) AS prev_time,
    lag(is_reversing) OVER (
      partition BY vg_id
      ORDER BY
        time ASC
    ) AS prev_is_reversing,
    -- Use current is_reversing to filter for changes in reversing
    time AS cur_time,
    is_reversing AS cur_is_reversing
  FROM
    reversing_interval_start_times
),
-- Filter to state transitions or edges (null)
reversing_state_transitions AS (
  SELECT
    *
  FROM
    previous_reversing_states
  WHERE
    prev_is_reversing != cur_is_reversing
    OR prev_is_reversing IS NULL
),
-- Looking forward to the start of the next transition
-- We do not fill in edges because reversing intervals should be short,
-- and we assume by default that a vehicle is not reversing.
reversing_state_transitions_with_next AS (
  SELECT
    org_id,
    vg_id,
    prev_time,
    cur_time,
    cur_is_reversing,
    lead(cur_time) OVER (
      partition BY vg_id
      ORDER BY
        cur_time ASC
    ) AS next_time,
    lead(cur_is_reversing) OVER (
      partition BY vg_id
      ORDER BY
        cur_time ASC
    ) AS next_is_reversing
  FROM
    reversing_state_transitions
),
-- Reversing intervals have been taken
reversing_intervals AS (
  SELECT
    org_id,
    vg_id,
    cur_time AS reversing_start_ms,
    next_time AS reversing_end_ms
  FROM
    reversing_state_transitions_with_next
  WHERE
    cur_is_reversing = true
),
-- recording intervals that overlap reversing intervals
recording_intervals_overlapping_reversing AS (
  SELECT
    reversing.org_id,
    reversing.vg_id,
    reversing_start_ms,
    reversing_end_ms,
    recording.start_ms,
    recording.end_ms
  FROM
    reversing_intervals AS reversing
    LEFT JOIN camera_connector_health.cc_recording_intervals AS recording
    ON reversing.vg_id = recording.vg_id
      AND recording.end_ms-recording.start_ms >= 1000
      AND NOT (
        reversing_start_ms >= recording.end_ms
        OR reversing_end_ms <= recording.start_ms
      )
  GROUP BY
    reversing.org_id,
    reversing.vg_id,
    reversing_start_ms,
    reversing_end_ms,
    recording.start_ms,
    recording.end_ms
),
-- recording intervals that overlap reversing intervals
recording_intervals_on_reversing_on_trips AS (
  SELECT
    org_id,
    vg_id,
    reversing_start_ms,
    reversing_end_ms,
    (reversing_end_ms - reversing_start_ms) AS reversing_duration_ms,
    CASE
      WHEN end_ms IS NOT NULL
      AND start_ms IS NOT NULL THEN least(reversing_end_ms, end_ms) - greatest(reversing_start_ms, recording.start_ms)
      ELSE 0
    END AS recording_interval_ms
  FROM
    recording_intervals_overlapping_reversing AS recording
) -- total recording duration per reversing interval
SELECT
  org_id,
  vg_id,
  date_format(
    from_unixtime(reversing_start_ms / 1000),
    'yyyy-MM-dd'
  ) AS date,
  reversing_start_ms,
  reversing_duration_ms,
  least(
    sum(recording_interval_ms),
    reversing_duration_ms
  ) AS recording_per_reversing_ms
FROM
  recording_intervals_on_reversing_on_trips
GROUP BY
  org_id,
  vg_id,
  reversing_start_ms,
  reversing_duration_ms
