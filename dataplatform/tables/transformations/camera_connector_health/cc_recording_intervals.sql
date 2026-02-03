
-- Start times of when the camera connector was recording with signal lock
WITH recording_interval_start_times AS (
  SELECT
    time,
    date,
    value,
    org_id,
    object_id AS cm_id,
    -- Only count as recording if has signal lock (int_value == 2)
    CASE
      WHEN value.int_value = 2 THEN true
      ELSE false
    END AS is_recording
  FROM
    kinesisstats.osdrearcameraframestate
  WHERE
    -- include any time if recording object stat within the last week
    date >= date_sub(${start_date}, 5)
    AND date < ${end_date}
      -- Exclude data in the past 4 hours due to ingestion delays.
    AND time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND value.is_databreak = false
    AND value.is_end = false
    AND value.is_start = false
),
-- All camera recording intervals with previous and next recording values
-- Used to combine stats where the recording state has not changed
previous_recording_states AS (
  SELECT
    org_id,
    cm_id,
    date,
    -- Add previous stat so we know the start of the range and if it was recording
    lag(time) OVER (
      partition BY cm_id
      ORDER BY
        time ASC
    ) AS prev_time,
    lag(is_recording) OVER (
      partition BY cm_id
      ORDER BY
        time ASC
    ) AS prev_is_recording,
    -- Use current is_recording to filter for changes in recording
    time AS cur_time,
    is_recording AS cur_is_recording
  FROM
    recording_interval_start_times
),
-- Filter to state transitions or edges (null)
recording_state_transitions AS (
  SELECT
    *
  FROM
    previous_recording_states
  WHERE
    prev_is_recording != cur_is_recording
    OR prev_is_recording IS NULL
),
-- Looking forward to the start of the next transition
recording_state_transitions_with_next AS (
  SELECT
    org_id,
    cm_id,
    CASE
      WHEN date_format(date, "yyyy-MM-dd") >= ${start_date}
      THEN date
      ELSE CAST(${start_date} AS string)
    END AS date,
    prev_time,
    COALESCE(prev_is_recording, cur_is_recording) AS prev_is_recording,
    CASE
        WHEN prev_time IS NULL
        THEN unix_timestamp(to_timestamp(${start_date})) * 1000
      ELSE cur_time
    END AS cur_time,
    cur_is_recording,
    COALESCE(
      lead(cur_time) OVER (
        partition BY cm_id
        ORDER BY
          cur_time ASC
      ),
      unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000
    ) AS next_time,
    lead(cur_is_recording) OVER (
      partition BY cm_id
      ORDER BY
        cur_time ASC
    ) AS next_is_recording
  FROM
    recording_state_transitions
),
-- Find linked VGs to join with connected intervals and reversing intervals
cm_linked_vgs AS (
  SELECT
    gateways.org_id,
    devices.id AS vg_device_id,
    devices.product_id AS product_id,
    gateways.device_id AS linked_cm_id,
    gateways.product_id AS cm_product_id
  FROM
    productsdb.gateways gateways
    JOIN productsdb.devices devices ON Upper(Replace(gateways.serial, '-', '')) = Upper(Replace(devices.camera_serial, '-', ''))
    AND devices.org_id = gateways.org_id
  WHERE
    gateways.product_id IN (
      43,  -- CM32
      44,  -- CM31
      155, -- CM34 BrigidDual-256GB
      167  -- CM33 BrigidSingle
    )
)
-- Select only when currently recording, along with linked VG
  SELECT
    recording.org_id,
    cm_id,
    cm_linked_vgs.vg_device_id as vg_id,
    date,
    cur_time AS start_ms,
    next_time AS end_ms
  FROM
    recording_state_transitions_with_next as recording
    INNER JOIN cm_linked_vgs
    ON recording.cm_id = cm_linked_vgs.linked_cm_id
  WHERE
    cur_is_recording = true
    AND cur_time >= unix_timestamp(to_timestamp(${start_date}))
