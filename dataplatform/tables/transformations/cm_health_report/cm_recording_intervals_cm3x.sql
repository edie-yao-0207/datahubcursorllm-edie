WITH osddashcamstate AS (
  SELECT
    org_id,
    object_id,
    time,
    date,
    value.int_value
  FROM
    kinesisstats.osddashcamstate
  WHERE
    osddashcamstate.value.is_databreak = false
    AND osddashcamstate.value.is_end = false
    AND date >= date_sub(${start_date}, 5)
    AND date < ${end_date}
    -- Exclude data in the past 4 hours due to ingestion delays.
    AND osddashcamstate.time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
),
cm_3x_recordings AS (
  SELECT
    cm_2x_3x_linked_vgs.org_id,
    cm_2x_3x_linked_vgs.vg_device_id,
    cm_2x_3x_linked_vgs.linked_cm_id,
    cm_2x_3x_linked_vgs.cm_product_id,
    osddashcamstate.time,
    CASE
      -- 1 = CameraState_CAMERA_RECORDING
      -- 6 = CameraState_CAMERA_RECORDING_DATA_ONLY
      -- 7 = CameraState_CAMERA_RECORDING_BUFFER_ONLY
      WHEN osddashcamstate.int_value IN (1, 6, 7) THEN 1
      ELSE 0
    END AS is_recording,
    osddashcamstate.date
  FROM
    cm_health_report.cm_2x_3x_linked_vgs AS cm_2x_3x_linked_vgs
    JOIN osddashcamstate ON cm_2x_3x_linked_vgs.org_id = osddashcamstate.org_id
    AND cm_2x_3x_linked_vgs.linked_cm_id = osddashcamstate.object_id
    AND cm_2x_3x_linked_vgs.cm_product_id IN (43, 44, 167, 155)
),
pairs AS (
  SELECT
    org_id,
    vg_device_id,
    linked_cm_id,
    cm_product_id,
    lag(time) OVER (
      PARTITION BY org_id,
      vg_device_id
      ORDER BY
        time ASC
    ) AS prev_time,
    --use this to see if the pair is the first item
    lag(is_recording) OVER (
      PARTITION BY org_id,
      vg_device_id
      ORDER BY
        time ASC
    ) AS prev_is_recording,
    time AS cur_time,
    is_recording AS cur_is_recording
  FROM
    cm_3x_recordings
),
-- filter down to points with recording change
transitions AS (
  SELECT
    org_id,
    vg_device_id,
    cm_product_id,
    prev_time,
    prev_is_recording,
    cur_time,
    cur_is_recording
  FROM
    pairs
  WHERE
    cur_is_recording != prev_is_recording
    OR (
      -- cm 3x reports every 90s, so end interval @ 90s; this is diff behavior from CM2x whose VG only report objstat on state change
      (cur_time - prev_time) >= 90 * 1000
      AND linked_cm_id IS NOT NULL
    )
    OR prev_time IS NULL
),
-- combine recording changes into intervals
intervals AS (
  SELECT
    org_id,
    vg_device_id,
    cm_product_id,
    cur_time AS start_ms,
    cur_is_recording AS is_recording,
    lead(cur_time) OVER (
      PARTITION BY org_id,
      vg_device_id
      ORDER BY
        cur_time
    ) AS end_ms
  FROM
    transitions
),
-- keep recording intervals only but exclude open interval at end
cm_3x_recording_intervals_closed AS (
  SELECT
    org_id,
    vg_device_id,
    is_recording,
    start_ms,
    end_ms
  FROM
    intervals
  WHERE
    end_ms IS NOT NULL
),
-- Select the latest dashcam stat per device for Cm3x. This will be used to close
-- the last open interval.
latest_cm_3x_recording AS (
  SELECT
    org_id,
    vg_device_id,
    MAX(time) as latest_time
  FROM
    cm_3x_recordings
  WHERE cm_product_id IN (43, 44, 167, 155)
  GROUP BY
    org_id,
    vg_device_id
),
cm_3x_recording_intervals_open AS (
  SELECT
    iv.org_id,
    iv.vg_device_id,
    iv.is_recording,
    iv.start_ms,
    latest.latest_time as end_ms -- close interval using the latest object stat
  FROM
    intervals as iv
    INNER JOIN latest_cm_3x_recording as latest
  ON iv.org_id = latest.org_id
    AND iv.vg_device_id = latest.vg_device_id
  WHERE
    end_ms IS NULL
    AND cm_product_id IN (43, 44, 167, 155)
),
cm_3x_recording_intervals AS (
  SELECT
    *
  FROM
    cm_3x_recording_intervals_closed
  UNION
  SELECT
    *
  FROM
    cm_3x_recording_intervals_open
),
target_with_disconnected_intervals AS (
  SELECT
    targets.org_id,
    targets.vg_device_id,
    targets.cm_device_id,
    targets.vg_product_id,
    targets.cm_product_id,
    targets.start_ms,
    targets.end_ms,
    targets.duration_ms,
    targets.date,
    targets.recording_expected,
    SUM (
      CASE
        WHEN disc.end_ms IS NOT NULL
        AND disc.start_ms IS NOT NULL THEN least(disc.end_ms, targets.end_ms) - greatest(disc.start_ms, targets.start_ms)
        ELSE 0
      END
    ) AS interval_disconnected_duration_raw
  FROM
    cm_health_report.cm_target_intervals AS targets
    LEFT JOIN cm_health_report.cm_physically_disconnected_intervals AS disc ON targets.org_id = disc.org_id
    AND targets.vg_device_id = disc.device_id
    AND (
      targets.cm_device_id = disc.linked_cm_id
      OR disc.linked_cm_id IS NULL
    )
    AND NOT (
      disc.start_ms >= targets.end_ms
      OR disc.end_ms <= targets.start_ms
    )
    AND targets.date >= ${start_date}
    AND targets.date < ${end_date}
    AND disc.date >= ${start_date}
    AND disc.date < ${end_date}
  GROUP BY
    targets.org_id,
    targets.vg_device_id,
    targets.cm_device_id,
    targets.cm_product_id,
    targets.vg_product_id,
    targets.start_ms,
    targets.end_ms,
    targets.duration_ms,
    targets.date,
    targets.recording_expected
),
target_with_connected_duration_intervals AS (
  SELECT
    *,
    duration_ms - interval_disconnected_duration_raw AS interval_connected_duration_ms
  FROM
    target_with_disconnected_intervals
),
cm_recording_durations AS (
  SELECT
    targets.org_id,
    targets.vg_device_id,
    targets.cm_device_id,
    targets.vg_product_id,
    targets.cm_product_id,
    targets.start_ms,
    targets.end_ms,
    targets.duration_ms,
    targets.date,
    targets.interval_connected_duration_ms,
    -- interval_recording_start_ms is earliest recording start on target segment
    greatest(MIN(recordings.start_ms), targets.start_ms) AS interval_recording_start_ms,
    targets.recording_expected,
    -- grace_recording_duration_ms as sum of recording interval durations on target segment
    SUM (
      CASE
        WHEN recordings.start_ms <= targets.start_ms
        AND recordings.end_ms >= targets.end_ms THEN targets.end_ms - targets.start_ms
        WHEN recordings.start_ms >= targets.start_ms
        AND recordings.end_ms <= targets.end_ms THEN recordings.end_ms - recordings.start_ms
        WHEN recordings.start_ms <= targets.start_ms
        AND recordings.end_ms <= targets.end_ms THEN recordings.end_ms - targets.start_ms
        WHEN recordings.start_ms >= targets.start_ms
        AND recordings.end_ms >= targets.end_ms THEN targets.end_ms - recordings.start_ms
        ELSE 0
      END
    ) AS grace_recording_duration_ms,
    SUM (
      CASE
        WHEN recordings.start_ms <= targets.start_ms
        AND recordings.end_ms >= targets.end_ms THEN targets.end_ms - targets.start_ms
        WHEN recordings.start_ms >= targets.start_ms
        AND recordings.end_ms <= targets.end_ms THEN recordings.end_ms - recordings.start_ms
        WHEN recordings.start_ms <= targets.start_ms
        AND recordings.end_ms <= targets.end_ms THEN recordings.end_ms - targets.start_ms
        WHEN recordings.start_ms >= targets.start_ms
        AND recordings.end_ms >= targets.end_ms THEN targets.end_ms - recordings.start_ms
        ELSE 0
      END
    ) AS recording_duration_ms
  FROM
    target_with_connected_duration_intervals targets
    LEFT JOIN cm_3x_recording_intervals recordings ON targets.org_id = recordings.org_id
    AND targets.vg_device_id = recordings.vg_device_id
    AND NOT (
      recordings.start_ms >= targets.end_ms
      OR recordings.end_ms <= targets.start_ms
    )
    AND recordings.is_recording = 1
    AND recordings.end_ms IS NOT NULL
    AND recordings.start_ms IS NOT NULL
  GROUP BY
    targets.org_id,
    targets.vg_device_id,
    targets.cm_device_id,
    targets.vg_product_id,
    targets.cm_product_id,
    targets.start_ms,
    targets.end_ms,
    targets.duration_ms,
    targets.date,
    targets.recording_expected,
    targets.interval_connected_duration_ms
),
cm_recording_durations_grace AS (
  SELECT
    org_id,
    vg_device_id,
    cm_device_id,
    vg_product_id,
    cm_product_id,
    start_ms,
    end_ms,
    duration_ms,
    date,
    interval_connected_duration_ms,
    interval_recording_start_ms,
    recording_expected,
    -- It can take up to 90s for the camera to turn on once a target begins, so allow a 90s grace period for recording intervals.
    CASE
      WHEN interval_recording_start_ms < start_ms + 90 * 1000 then grace_recording_duration_ms + interval_recording_start_ms - start_ms
      ELSE grace_recording_duration_ms
    END AS grace_recording_duration_ms,
    recording_duration_ms
  FROM
    cm_recording_durations
),
cm_recording_durations_with_bootcount AS (
  SELECT
    r.*,
    COALESCE(bootcount.vg_bootcount, 0) AS vg_bootcount,
    bootcount.vg_build AS vg_build,
    COALESCE(bootcount.cm_bootcount, 0) AS cm_bootcount,
    bootcount.cm_build AS cm_build
  FROM
    cm_recording_durations_grace AS r
    LEFT JOIN cm_health_report.cm_target_build_and_bootcount AS bootcount ON r.org_id = bootcount.org_id
    AND r.vg_device_id = bootcount.vg_device_id
    AND r.start_ms = bootcount.start_ms
    AND r.end_ms = bootcount.end_ms
)
SELECT
  *
FROM
  cm_recording_durations_with_bootcount
WHERE
  -- HACK: This node is producing start/end dates out of range. For now, filter the
  -- output data to be within start/end dates and investigate as a followup.
  -- https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#setting/sparkui/0714-221715-cross910/driver-logs
  date >= ${start_date}
  AND date < ${end_date}
  AND cm_product_id IN (43, 44, 167, 155)
