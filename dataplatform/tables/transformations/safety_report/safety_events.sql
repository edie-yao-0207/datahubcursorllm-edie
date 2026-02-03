WITH safety_events AS (
  SELECT
    events.org_id,
    events.device_id,
    -- There is a known issue where safetydb_shards.safety_events.driver_id is sometimes less accurate than driver_id in the tripsdb.trips and
    -- trips2db.trips tables. Prefer to read driver_id from trips table here.
    trips.driver_id_exclude_0_drivers as driver_id,
    events.trip_start_ms,
    trips.end_ms as trip_end_ms,
    trips.version as trip_version,
    events.event_ms,
    -- in safety_report we group safety events by trip_end_ms
    TO_DATE(
      from_unixtime(
        trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
      )
    ) as date,
    CAST( case when metadata.coaching_state in (3,6,8,11,12) then 1 else 0 end AS BYTE) as in_dismissed_states,
    CAST( case when metadata.coaching_state in (9,10) then 1 else 0 end AS BYTE) as in_recognition_states,
    CAST( case when metadata.coaching_state in (2,7) then 1 else 0 end AS BYTE) as in_coaching_states,
    CAST( case when metadata.coaching_state in (1,4) then 1 else 0 end AS BYTE) as in_triage_states,
    -- 100 is used to indicate match safetyreportingdb_shards.triage_events_reporting_release_stage.release_stage
    CAST( case when events.release_stage in (4) then 100 else 1 end AS SHORT) as release_stage,
    events.additional_labels.additional_labels AS additional_labels,
    detail_proto.event_id AS event_id
  FROM
    safetydb_shards.safety_events events
    JOIN safetydb_shards.safety_event_metadata metadata ON events.org_id = metadata.org_id
    AND events.device_id = metadata.device_id
    AND events.event_ms = metadata.event_ms
    AND events.date = metadata.date
    JOIN trips2db_shards.trips trips ON trips.device_id = events.device_id
    AND trips.org_id = events.org_id
    AND trips.start_ms = events.trip_start_ms
    JOIN clouddb.groups groups ON groups.organization_id = events.org_id
  WHERE
    (
      --Filter out events that happen under 5 mph unless
      --they are haCrash (5) or haRolledStopSign (11) or haRollover(28) or haSeatbeltPolicy(23)
      --or HEv2 (ingestion_tag = 1)
      detail_proto.start.speed_milliknots >= 4344.8812095
      AND detail_proto.stop.speed_milliknots >= 4344.8812095
      OR detail_proto.accel_type IN (5, 11, 23, 28)
      OR detail_proto.ingestion_tag = 1
    )
    --We want to subtract 2 days from the safety_events and trips start_date because trips can last up to 48 hours,
    --so we need to include all trips from 48 hours before the start date until the end date to accurately fetch all safety
    --events within the selected period.
    AND events.date >= DATE_ADD(${start_date}, -2)
    AND events.date < ${end_date}
    AND trips.date >= DATE_ADD(${start_date}, -2)
    AND trips.date < ${end_date}
    AND TO_DATE(
      from_unixtime(trips.end_ms / CAST(1e3 AS DECIMAL(4, 0)))
    ) >= ${start_date}
    AND TO_DATE(
      from_unixtime(trips.end_ms / CAST(1e3 AS DECIMAL(4, 0)))
    ) < ${end_date}
    --A trip is considered part of an unassigned group when org id or group id is 1 (US) or 562949953421343 (EU).
    --Filter out trips corresponding to unassigned orgs.
    AND trips.org_id NOT IN (1, 562949953421343) --Filter out trips with invalid trip version
    AND trips.version = 101
)
SELECT
  org_id,
  device_id,
  driver_id,
  event_ms,
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
  date
FROM
  safety_events
