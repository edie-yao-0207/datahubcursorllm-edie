-- Hardcode hour buckets (in ms) for a 24 hour period. This will enable us to split target
-- durations on the hour. Targets will be joined on this table to be split into <=1hr long
-- with sub targets with start_ms, end_ms, and duration_ms adjusted accordingly.
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
osddashcamtargetstate AS (
  SELECT
    org_id,
    object_id as device_id,
    time,
    date,
    value.is_databreak as is_databreak,
    value.proto_value.dashcam_target_state.recording_reason as recording_reason,
    value.proto_value.dashcam_target_state.camera_target_state as camera_target_state_value
  FROM
    kinesisstats.osddashcamtargetstate
  WHERE
    osddashcamtargetstate.date >= date_sub(${start_date}, 5)
    and osddashcamtargetstate.date < ${end_date}
    -- Exclude data in the past 4 hours due to ingestion delays.
    AND osddashcamtargetstate.time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
),
pairs AS (
  SELECT
    org_id,
    device_id,
    date,
    time,
    recording_reason,
    camera_target_state_value,

    lag(is_databreak) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time ASC
    ) AS prev_is_databreak,
    lag(recording_reason) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time ASC
    ) AS prev_recording_reason,
    lag(camera_target_state_value) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time ASC
    ) AS prev_target_state_value,
    lead(is_databreak) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time ASC
    ) as next_is_databreak,
    lead(recording_reason) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time ASC
    ) AS next_recording_reason,
    lead(camera_target_state_value) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time ASC
    ) AS next_target_state_value,
    !(is_databreak OR next_is_databreak)
      AND (prev_target_state_value IS NULL OR (recording_reason != prev_recording_reason) OR (camera_target_state_value != prev_target_state_value) OR prev_is_databreak) AS is_start,
    !(is_databreak OR prev_is_databreak)
      AND (next_target_state_value IS NULL OR (recording_reason != prev_recording_reason) OR (camera_target_state_value != prev_target_state_value) OR next_is_databreak) as is_end
  FROM
    osddashcamtargetstate
), transitions AS (
    select *,
            lead(CASE WHEN is_end THEN time ELSE NULL END) OVER (
                PARTITION BY org_id, device_id
                ORDER BY time ASC
            ) as end_ms_or_null
    from pairs
    where is_start OR is_end
), intervals AS (
    select org_id, date, device_id, time as start_ms, end_ms_or_null, recording_reason, camera_target_state_value as target_state
    from transitions
    where is_start
),
cm_2x_3x_linked_vgs_ranked_by_camera_first_connected_at_ms as (
  select  *, ROW_NUMBER() OVER (PARTITION BY linked_cm_id ORDER BY camera_first_connected_at_ms desc) as rn
  from    cm_health_report.cm_2x_3x_linked_vgs
),
current_cm_2x_3x_linked_vgs as (
  select  *
  from    cm_2x_3x_linked_vgs_ranked_by_camera_first_connected_at_ms
  where   rn = 1
),
target_intervals as (
  select  ts.org_id,
          cm_2x_3x_linked_vgs.vg_device_id as vg_device_id,
          cm_2x_3x_linked_vgs.product_id as vg_product_id,
          cm_2x_3x_linked_vgs.linked_cm_id as cm_device_id,
          cm_2x_3x_linked_vgs.cm_product_id,
          ts.start_ms, ts.end_ms_or_null as end_ms,
          ts.end_ms_or_null - ts.start_ms as duration_ms, ts.date,
          CASE
              WHEN ts.target_state = 2 THEN True
              ELSE False
          END AS recording_expected,
          ts.recording_reason
  from intervals AS ts
  join current_cm_2x_3x_linked_vgs AS cm_2x_3x_linked_vgs on cm_2x_3x_linked_vgs.org_id = ts.org_id
      AND cm_2x_3x_linked_vgs.linked_cm_id = ts.device_id
      AND ts.start_ms >= cm_2x_3x_linked_vgs.camera_first_connected_at_ms
  WHERE ts.start_ms <> ts.end_ms_or_null
),
split_target_intervals AS (
  SELECT
    org_id,
    vg_device_id,
    vg_product_id,
    cm_device_id,
    cm_product_id,
    intervals.start_ms + t.split_start_ms AS start_ms,
    intervals.start_ms + least(t.split_end_ms, intervals.duration_ms) AS end_ms,
    least(
      --(1hr, remainder of last segment)
      t.split_end_ms - t.split_start_ms,
      greatest(0, intervals.duration_ms - t.split_start_ms)
    ) AS duration_ms,
    recording_expected,
    recording_reason,
    intervals.duration_ms AS original_duration_ms,
    -- Write all split targets with the date of the original interval, even
    -- if they cross midnight. This is because the original unsplit target
    -- is written to the date corresponding to interval start. In order
    -- for split targets to fall under the consistently correct date partition,
    -- they must be written to the same partition as the source data.
    from_unixtime(
      intervals.start_ms / 1000,
      "yyyy-MM-dd"
    ) AS date,
    t.split_start_ms,
    t.split_end_ms
  FROM
    target_intervals intervals
    JOIN split_times_ms t ON intervals.duration_ms > t.split_start_ms
  WHERE
    recording_expected = true
), output_data AS (
  -- Union target intervals, non target intervals, & recent non target intervals
  -- This gives us all target & non target intervals for a device
  SELECT
    org_id,
    vg_device_id,
    vg_product_id,
    cm_device_id,
    cm_product_id,
    start_ms,
    end_ms,
    duration_ms,
    date,
    recording_expected,
    recording_reason
  FROM
    target_intervals
  WHERE
    recording_expected = false
  UNION
  SELECT
    org_id,
    vg_device_id,
    vg_product_id,
    cm_device_id,
    cm_product_id,
    start_ms,
    end_ms,
    duration_ms,
    date,
    recording_expected,
    recording_reason
  FROM
    split_target_intervals
)

select  d.*
from    output_data d
WHERE   d.date >= ${start_date}
    AND d.date < ${end_date}
