WITH triage_v1_safety_events_v2 AS (
  SELECT
    sev2.org_id,
    sev2.device_id,
    trips.driver_id_exclude_0_drivers as driver_id,
    sev2.trip_start_ms,
    trips.end_ms as trip_end_ms,
    trips.version as trip_version,
    sev2.start_ms as event_ms,
    sev2.end_ms - sev2.start_ms as event_duration,
    -- in safety_report we group safety events by trip_end_ms
    TO_DATE(
      from_unixtime(
        trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
      )
    ) as date,
    CAST( case when tev1.triage_state in (1, 2) and tev1.is_dismissed = 0 then 1 else 0 end AS BYTE) as in_triage_states,
    CAST( case when tev1.triage_state in (3) and tev1.is_dismissed = 0 then 1 else 0 end AS BYTE) as in_coaching_states,
    CAST(tev1.is_dismissed AS BYTE)  as in_dismissed_states,
    CAST( 0 AS BYTE ) as in_recognition_states,   -- no recognition state in triage events
    CAST( reporting_release_stage.release_stage AS SHORT ) as release_stage,
    case when tev1.trigger_label not in (0) then ARRAY(
      STRUCT(
        CAST ( tev1.trigger_label AS INTEGER ) as label_type,
        1 as label_source,                     -- label_source = 1 indicates system
        sev2.created_at_ms as last_added_ms,   -- setting event creation time as added time
        CAST ( 0 AS LONG) as user_id           -- 0 indicates Samsara
      )
    ) else ARRAY(null) end as additional_labels,
    sev2.event_id as event_id,
    tev1.uuid as triage_uuid,
    1 as triage_version             -- 1 is used to indicate it is from triage_events v1 table
  FROM
    safetyeventtriagedb_shards.triage_events tev1
    INNER JOIN safetyeventingestiondb_shards.safety_events_v2 sev2
    ON tev1.org_id = sev2.org_id
    AND tev1.device_id = sev2.device_id
    AND tev1.start_ms = sev2.start_ms
    AND tev1.source_event_uuid = sev2.uuid
    JOIN trips2db_shards.trips trips
    ON sev2.org_id = trips.org_id
    AND sev2.device_id = trips.device_id
    AND  sev2.trip_start_ms = trips.start_ms
    JOIN clouddb.groups groups
    ON tev1.org_id = groups.organization_id
    INNER JOIN safetyreportingdb_shards.triage_events_reporting_release_stage reporting_release_stage
    ON tev1.org_id = reporting_release_stage.org_id
    AND tev1.uuid = reporting_release_stage.event_uuid
  WHERE
    --We want to subtract 2 days from the safety_events and trips start_date because trips can last up to 48 hours,
    --so we need to include all trips from 48 hours before the start date until the end date to accurately fetch all safety
    --events within the selected period.
    TO_DATE(
      from_unixtime(sev2.start_ms / CAST(1e3 AS DECIMAL(4, 0)))
    ) >= DATE_ADD(${start_date}, -2)
    AND TO_DATE(
      from_unixtime(sev2.start_ms / CAST(1e3 AS DECIMAL(4, 0)))
    ) < ${end_date}
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
    -- Update this as we are ready to roll out triage events for other labels than max speeding.
    AND tev1.trigger_label IN (32)
),

triage_v2_safety_events_v2 AS (
  SELECT
    sev2.org_id,
    sev2.device_id,
    trips.driver_id_exclude_0_drivers as driver_id,
    sev2.trip_start_ms,
    trips.end_ms as trip_end_ms,
    trips.version as trip_version,
    sev2.start_ms as event_ms,
    sev2.end_ms - sev2.start_ms as event_duration,
    -- in safety_report we group safety events by trip_end_ms
    TO_DATE(
      from_unixtime(
        trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
      )
    ) as date,
    CAST( case when tev2.triage_state in (1, 2) then 1 else 0 end AS BYTE) as in_triage_states,
    CAST( case when tev2.triage_state in (3) then 1 else 0 end AS BYTE) as in_coaching_states,
    CAST( case when tev2.triage_state in (4) then 1 else 0 end AS BYTE) as in_dismissed_states,
    CAST( 0 AS BYTE ) as in_recognition_states,   -- no recognition state in triage events
    CAST( reporting_release_stage.release_stage AS SHORT ) as release_stage,
    -- TODO - update this field as we better understand what behavior labels will look like in tev2
    case when tev2.trigger_label not in (0) then ARRAY(
      STRUCT(
        CAST ( tev2.trigger_label AS INTEGER ) as label_type,
        1 as label_source,                     -- label_source = 1 indicates system
        sev2.created_at_ms as last_added_ms,   -- setting event creation time as added time
        CAST ( 0 AS LONG) as user_id           -- 0 indicates Samsara
      )
    ) else ARRAY(null) end as additional_labels,
    sev2.event_id as event_id,
    tev2.uuid as triage_uuid,
    2 as triage_version             -- 2 is used to indicate it is from triage_events_v2 table
  FROM
    safetyeventtriagedb_shards.triage_events_v2 tev2
    INNER JOIN safetyeventingestiondb_shards.safety_events_v2 sev2
    ON tev2.org_id = sev2.org_id
    AND tev2.device_id = sev2.device_id
    AND tev2.start_ms = sev2.start_ms
    AND tev2.source_event_uuid = sev2.uuid
    JOIN trips2db_shards.trips trips
    ON sev2.org_id = trips.org_id
    AND sev2.device_id = trips.device_id
    AND  sev2.trip_start_ms = trips.start_ms
    JOIN clouddb.groups groups
    ON tev2.org_id = groups.organization_id
    INNER JOIN safetyreportingdb_shards.triage_events_reporting_release_stage reporting_release_stage
    ON tev2.org_id = reporting_release_stage.org_id
    AND tev2.uuid = reporting_release_stage.event_uuid
  WHERE
    tev2.version = 2 AND
    --We want to subtract 2 days from the safety_events and trips start_date because trips can last up to 48 hours,
    --so we need to include all trips from 48 hours before the start date until the end date to accurately fetch all safety
    --events within the selected period.
    TO_DATE(
      from_unixtime(sev2.start_ms / CAST(1e3 AS DECIMAL(4, 0)))
    ) >= DATE_ADD(${start_date}, -2)
    AND TO_DATE(
      from_unixtime(sev2.start_ms / CAST(1e3 AS DECIMAL(4, 0)))
    ) < ${end_date}
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
    -- Update this as we are ready to roll out triage events for other labels than max speeding.
    AND tev2.trigger_label IN (32)
),

triage_v2_safety_events_v1 AS (
  SELECT
    sev1.org_id,
    sev1.device_id,
    trips.driver_id_exclude_0_drivers AS driver_id,
    sev1.trip_start_ms,
    trips.end_ms AS trip_end_ms,
    trips.version as trip_version,
    sev1.event_ms,
     -- this field is not applicable to safety event v1 events, but used for v2 events
    0 as event_duration,
    TO_DATE(
      from_unixtime(
        trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
      )
    ) as date, -- in safety_report we group safety events by trip_end_ms

    ---- Triage states ---
    CAST( case when metadata.coaching_state in (3,6,8,11,12) then 1 else 0 end AS BYTE) as in_dismissed_states,
    CAST( case when metadata.coaching_state in (2,7) then 1 else 0 end AS BYTE) as in_coaching_states,
    CAST( case when metadata.coaching_state in (1,4) then 1 else 0 end AS BYTE) as in_triage_states,
    CAST( case when metadata.coaching_state in (9,10) then 1 else 0 end AS BYTE) as in_recognition_states,

    -- Release stage --
    -- 100 is used to indicate match safetyreportingdb_shards.triage_events_reporting_release_stage.release_stage
    CAST( case when sev1.release_stage in (4) then 100 else 1 end AS SHORT) as release_stage,

    -- Additional Labels --
     COALESCE(case when tev2.trigger_label not in (0) then
      TRANSFORM(sev1.additional_labels.additional_labels, x ->
        STRUCT(
          x.label_type,
          COALESCE(x.label_source, 1),
          x.last_added_ms,
          COALESCE(x.user_id, 0)
        )
      )
    else ARRAY(null) end,
    ARRAY(null)
    ) as additional_labels,

    detail_proto.event_id AS event_id,
    tev2.uuid AS triage_uuid,
    -- 2 is used to indicate it is from triage_events_v2 table
    2 AS triage_version

FROM safetyeventtriagedb_shards.triage_events_v2 tev2
JOIN safetydb_shards.safety_events sev1
  ON tev2.org_id = sev1.org_id
  AND tev2.device_id = sev1.device_id
  AND tev2.start_ms = sev1.event_ms
  -- source_event_uuid from tev2 is only for SafetyEventV2.Uuid so we don't join here
JOIN safetydb_shards.safety_event_metadata metadata ON sev1.org_id = metadata.org_id
  AND sev1.device_id = metadata.device_id
  AND sev1.event_ms = metadata.event_ms
  AND sev1.date = metadata.date
JOIN trips2db_shards.trips trips
  ON sev1.org_id = trips.org_id
  AND sev1.device_id = trips.device_id
  AND sev1.trip_start_ms = trips.start_ms

JOIN clouddb.groups groups
  ON tev2.org_id = groups.organization_id

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
  AND tev2.version = 1 --Version of the triage event to assist migration strategy. (Safety Event V1 or V2)
  AND sev1.date >= DATE_ADD(${start_date}, -2)
  AND sev1.date < ${end_date}
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
  AND trips.org_id NOT IN (1, 562949953421343)
  --Filter out trips with invalid trip version
  AND trips.version = 101
),

triage_safety_events AS (
  SELECT * FROM triage_v1_safety_events_v2
  UNION
  SELECT * FROM triage_v2_safety_events_v2
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
  release_stage,
  event_id,
  triage_uuid,
  date,
  triage_version
FROM
  triage_safety_events
