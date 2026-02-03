WITH safety_event_trip_labels_v2 AS (
  SELECT org_id,
       device_id,
       driver_id,
       event_ms,
       event_duration,
       trip_start_ms,
       trip_end_ms,
       trip_version,
       EXPLODE(additional_labels) AS additional_label,
       in_dismissed_states,
       in_recognition_states,
       in_coaching_states,
       in_triage_states,
       release_stage,
       triage_uuid,
       date
  FROM safety_report.safety_events_deduplicated
  WHERE date >= ${start_date}
  AND date < ${end_date}
)

SELECT org_id,
       device_id,
       driver_id,
       event_ms,
       event_duration,
       trip_start_ms,
       trip_end_ms,
       trip_version,
       additional_label.label_type   as label_type,
       additional_label.label_source as label_source,
       in_dismissed_states,
       in_recognition_states,
       in_coaching_states,
       in_triage_states,
       release_stage,
       triage_uuid,
       date
FROM safety_event_trip_labels_v2
