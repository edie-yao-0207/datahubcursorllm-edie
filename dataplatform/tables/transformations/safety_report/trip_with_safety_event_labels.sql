-- select the earliest safety event label_type for each trip
-- we do NOT differentiate between manual and automatic
-- we only count GA labels
-- This differes versus trip_with_safety_event_labels in
-- that we are removing the coaching_state field and returning
-- boolean flags indicating the range of states its in.
WITH trip_with_safety_events_label AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY org_id,
      device_id,
      trip_end_ms,
      label_type
      ORDER BY
        event_ms ASC
    ) AS row_number
  FROM
    safety_report.safety_event_labels labels
    -- safety_report.safety_event_labels.date is base on trip_end_ms
  WHERE
    labels.date >= ${start_date}
    AND labels.date < ${end_date}
)
SELECT
  org_id,
  device_id,
  driver_id,
  event_ms,
  trip_start_ms,
  trip_end_ms,
  trip_version,
  label_type,
  label_source,
  in_dismissed_states,
  in_recognition_states,
  in_coaching_states,
  in_triage_states,
  release_stage,
  date
FROM
  trip_with_safety_events_label
WHERE
  row_number = 1
