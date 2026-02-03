SELECT
  cm2x_intervals.org_id,
  cm2x_intervals.device_id,
  cm2x_intervals.cm_device_id,
  cm2x_intervals.product_id,
  cm2x_intervals.cm_product_id,
  cm2x_intervals.start_ms,
  cm2x_intervals.end_ms,
  cm2x_intervals.duration_ms,
  cm2x_intervals.date,
  cm2x_intervals.interval_connected_duration_ms,
  cm2x_intervals.interval_recording_start_ms,
  cm2x_intervals.grace_recording_duration_ms,
  cm2x_intervals.recording_duration_ms,
  COALESCE(cm2x_intervals.vg_bootcount, 0) AS vg_bootcount,
  cm2x_intervals.vg_build,
  COALESCE(cm2x_intervals.cm_bootcount, 0) AS cm_bootcount,
  cm2x_intervals.cm_build
FROM
  cm_health_report.cm_recording_intervals_cm2x as cm2x_intervals
WHERE cm2x_intervals.on_trip = true
  -- HACK: This node is producing start/end dates out of range. For now, filter the
  -- output data to be within start/end dates and investigate as a followup.
  -- https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#setting/sparkui/0714-221715-cross910/driver-logs
  AND date >= ${start_date}
  AND date < ${end_date}

UNION

SELECT
  cm3x_intervals.org_id,
  cm3x_intervals.vg_device_id as device_id,
  cm3x_intervals.cm_device_id,
  cm3x_intervals.vg_product_id as product_id,
  cm3x_intervals.cm_product_id,
  cm3x_intervals.start_ms,
  cm3x_intervals.end_ms,
  cm3x_intervals.duration_ms,
  cm3x_intervals.date,
  cm3x_intervals.interval_connected_duration_ms,
  cm3x_intervals.interval_recording_start_ms,
  cm3x_intervals.grace_recording_duration_ms,
  cm3x_intervals.recording_duration_ms,
  COALESCE(cm3x_intervals.vg_bootcount, 0) AS vg_bootcount,
  cm3x_intervals.vg_build,
  COALESCE(cm3x_intervals.cm_bootcount, 0) AS cm_bootcount,
  cm3x_intervals.cm_build
FROM
  cm_health_report.cm_recording_intervals_cm3x as cm3x_intervals
WHERE cm3x_intervals.recording_expected = true
  -- HACK: This node is producing start/end dates out of range. For now, filter the
  -- output data to be within start/end dates and investigate as a followup.
  -- https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#setting/sparkui/0714-221715-cross910/driver-logs
  AND date >= ${start_date}
  AND date < ${end_date}
