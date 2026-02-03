WITH safety_events_with_interval AS (
  SELECT
    *,
    WINDOW (
    FROM_UNIXTIME(trip_end_ms / CAST(1e3 AS DECIMAL(4,0))),
    '1 hour'
).start AS interval_start
FROM
  safety_report.safety_events_deduplicated_report_v3
WHERE
  -- safety_report.safety_events_deduplicated.date is base on trip_end_ms
    date >= ${start_date}
  AND date < ${end_date}
  ),

safety_events_device_interval AS (
  SELECT
    org_id,
    device_id AS object_id,
    1 AS object_type,
    event_id,
    interval_start,
    trip_version,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    release_stage,
    date
  FROM
    safety_events_with_interval
),

safety_events_driver_interval AS (
  SELECT
    org_id,
    -- driver id could be null
    COALESCE(driver_id, 0) AS object_id,
    5 AS object_type,
    event_id,
    interval_start,
    trip_version,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    release_stage,
    date
  FROM
    safety_events_with_interval
),

object_safety_events_interval AS (
  SELECT
    *
  FROM
    safety_events_device_interval
  union all
  SELECT
    *
  FROM
    safety_events_driver_interval
),

object_safety_events_interval_agg AS (
  SELECT
    org_id,
    object_id,
    object_type,
    interval_start,
    trip_version,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    release_stage,
    count(distinct object_id, object_type, event_id) as total_event_count,
    date
  FROM
    object_safety_events_interval
  GROUP BY
    org_id,
    object_id,
    object_type,
    interval_start,
    trip_version,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    release_stage,
    date
)

select * from object_safety_events_interval_agg
