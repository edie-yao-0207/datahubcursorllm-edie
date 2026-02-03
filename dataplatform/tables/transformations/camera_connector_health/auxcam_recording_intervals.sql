

-- Only include auxcams that are associated with a VG
with auxcams_with_associations as (
  select
    distinct device_id as auxcam_id
  from
    deviceassociationsdb_shards.device_collection_associations
  where
    product_id = 126
    and end_at is null
),
recording_interval_start_times AS (
  SELECT
    time,
    date,
    value,
    org_id,
    object_id AS auxcam_id,
    CASE
      -- 1 = CameraState_CAMERA_RECORDING
      -- 6 = CameraState_CAMERA_RECORDING_DATA_ONLY
      -- 7 = CameraState_CAMERA_RECORDING_BUFFER_ONLY
      WHEN value.int_value IN (1, 6, 7) THEN true
      ELSE false
    END AS is_recording
  FROM
    kinesisstats.osddashcamstate
  WHERE
    -- include any time if recording object stat within the last week
    date >= ${start_date}
    AND date < ${end_date}
      -- Exclude data in the past 4 hours due to ingestion delays.
    AND time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
    AND value.is_databreak = false
    AND value.is_end = false
    AND value.is_start = false
    AND object_id in (select auxcam_id from auxcams_with_associations)
),
-- Find the latest recording time per auxcam
-- This will be used to close off the last open recording interval
latest_recording_time_per_auxcam as (
  select
    max(time) as latest_recording_time,
    auxcam_id
  from
    recording_interval_start_times
  where
    is_recording = true
  group by
    auxcam_id
),
-- All auxcam recording intervals with previous and next recording values
-- Used to combine stats where the recording state has not changed
previous_recording_states AS (
  SELECT
    org_id,
    auxcam_id,
    date,
    -- Add previous stat so we know the start of the range and if it was recording
    lag(time) OVER (
      partition BY auxcam_id
      ORDER BY
        time ASC
    ) AS prev_time,
    lag(is_recording) OVER (
      partition BY auxcam_id
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
-- Fills in where cur_is_recording is null with the latest recording
recording_state_transition_with_latest AS (
  SELECT
    org_id,
    l.auxcam_id,
    date,
    prev_time,
    prev_is_recording,
    cur_time,
    cur_is_recording,
    latest_recording_time
  FROM
    recording_state_transitions r
    inner join latest_recording_time_per_auxcam l on r.auxcam_id=l.auxcam_id
),
-- Looking forward to the start of the next transition
recording_state_transitions_with_next AS (
  SELECT
    org_id,
    auxcam_id,
    CASE
      WHEN date_format(date, "yyyy-MM-dd") >= ${start_date}
      THEN date
      ELSE CAST(${start_date} AS string)
    END AS date,
    cur_time,
    cur_is_recording,
    COALESCE(
      lead(cur_time) OVER (
        partition BY auxcam_id
        ORDER BY
          cur_time ASC
      ),
      latest_recording_time + 60 * 1000
    ) AS next_time,
    lead(cur_is_recording) OVER (
      partition BY auxcam_id
      ORDER BY
        cur_time ASC
    ) AS next_is_recording
  FROM
    recording_state_transition_with_latest
)
-- Select only when auxcam is recording
  SELECT
    recording.org_id,
    auxcam_id,
    date,
    cur_time AS start_ms,
    next_time AS end_ms
  FROM
    recording_state_transitions_with_next as recording
  WHERE
    cur_is_recording = true
    AND cur_time >= unix_timestamp(to_timestamp(${start_date}))
    AND next_time IS NOT NULL
