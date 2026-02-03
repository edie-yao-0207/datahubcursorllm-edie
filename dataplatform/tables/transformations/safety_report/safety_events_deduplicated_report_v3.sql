-- We will union all the sources in this query
-- 1 will be events in triage events v1
-- 2 will be events from safety events v1
-- 3 will be events in triage event v2
-- These correlate with the priority when deduplicating (1 being the highest)
-- These should be reordered once safety events are backfilled into triage events.
WITH all_safety_events AS (
  SELECT
  org_id,
  device_id,
  driver_id,
  event_ms,
  0 as event_duration,    -- this field is not applicable to safety event v1 events, but used for v2 events
  trip_start_ms,
  trip_end_ms,
  trip_version,
  additional_labels,
  in_dismissed_states,
  in_recognition_states,
  in_coaching_states,
  in_triage_states,
  release_stage,
  event_id,
  date,
  NULL AS triage_uuid,
  2 AS source_table,
  -- Add mock model_version data with inward_mtl_cm variations
 CAST(CASE
    WHEN event_id IS NULL THEN 'inward_mtl_cm_unknown'
    WHEN MOD(CAST(event_id AS BIGINT), 3) = 0 THEN 'inward_mtl_cm_v8.0.0'
    WHEN MOD(CAST(event_id AS BIGINT), 3) = 1 THEN 'inward_mtl_cm_v8.1.0'
    ELSE 'inward_mtl_cm_v8.2.0'
  END AS STRING) AS model_version
  FROM safety_report.safety_events
  WHERE date >= ${start_date}
  AND date < ${end_date}

  UNION

  SELECT
  org_id,
  device_id,
  driver_id,
  event_ms,
  event_duration,
  trip_start_ms,
  trip_end_ms,
  trip_version,
  additional_labels,
  in_dismissed_states,
  in_recognition_states,
  in_coaching_states,
  in_triage_states,
  release_stage,
  event_id,
  date,
  triage_uuid,
  -- triage_version is used to indicate it is from triage_events v1 or triage_ v2 table
  -- Set priority to 3 for triage_events_v1
  -- Set priority to 1 for triage_events_v2
  CAST(
   CASE WHEN tsev2.triage_version = 1 THEN 3 ELSE 1 END
  AS SMALLINT) AS source_table,
  CAST(CASE
    WHEN event_id IS NULL THEN 'inward_mtl_cm_unknown'
    WHEN MOD(CAST(event_id AS BIGINT), 3) = 0 THEN 'inward_mtl_cm_v8.0.0'
    WHEN MOD(CAST(event_id AS BIGINT), 3) = 1 THEN 'inward_mtl_cm_v8.1.0'
    ELSE 'inward_mtl_cm_v8.2.0'
  END AS STRING) AS model_version
  FROM safety_report.triage_safety_events_v2_report_v3 as tsev2
  WHERE date >= ${start_date}
  AND date < ${end_date}
),

-- Select the highest priority source table for each org and event_id
all_safety_events_priority_order AS (
  SELECT *,
  row_number() over (partition by org_id, event_id order by source_table asc) AS row_num
  FROM all_safety_events
),

safety_events_deduplicated_report_v3 AS (
  SELECT *
  FROM all_safety_events_priority_order
  WHERE row_num=1
)

SELECT
  org_id,
  device_id,
  driver_id,
  event_ms,
  event_duration,
  trip_start_ms,
  trip_end_ms,
  trip_version,
  additional_labels,
  in_dismissed_states,
  in_recognition_states,
  in_coaching_states,
  in_triage_states,
  CAST(release_stage AS SHORT) as release_stage,
  event_id,
  triage_uuid,
  date,
  model_version
FROM
  safety_events_deduplicated_report_v3





