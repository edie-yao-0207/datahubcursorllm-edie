-- Purpose: Parameterised SQL query that powers event detection E2E latency alert.
--
-- How it works:
-- The query uses 'event_ms' (timestamp recorded on the device) and 'created_at' (timestamp recorded upon saving as an event in SafetyDB) to calculate the latency of each event.
--
-- To add a new event type, simply add another WHEN clause with the event type and the corresponding threshold.
WITH latency AS (
  SELECT
    a.event_type,
    DATE_FORMAT(DATE_TRUNC('DAY', s.created_at), 'yyyy-MM-dd') AS date,
    ROUND(
      PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY
          (
            to_unix_timestamp(s.created_at) - s.event_ms / 1000
          )
      ) / 60,
      2
    ) as p50_latency_mins,
  CASE
     WHEN event_type = 'haAccel' THEN 2
     WHEN event_type = 'haOutwardObstructionPolicy' THEN 15
     WHEN event_type = 'haCameraMisaligned' THEN 25
     WHEN event_type = 'haTailgating' THEN 5
     WHEN event_type = 'haBraking' THEN 2
     WHEN event_type = 'haDriverObstructionPolicy' THEN 3
     WHEN event_type = 'haTileRollingStopSign' THEN 7
     WHEN event_type = 'haNearCollision' THEN 7
     WHEN event_type = 'haDrowsinessDetection' THEN 5
     WHEN event_type = 'haSharpTurn' THEN 2
     WHEN event_type = 'haLaneDeparture' THEN 8
     WHEN event_type = 'haPhonePolicy' THEN 5
     WHEN event_type = 'haDistractedDriving' THEN 5
     WHEN event_type = 'haSeatbeltPolicy' THEN 5
     WHEN event_type = 'haCrash' THEN 2
     -- add another event_type and its threshold here
     ELSE -1
  END AS latency_threshold_mins
  FROM
    safetydb_shards.safety_events s
    INNER JOIN definitions.harsh_accel_type_enums a ON a.enum = s.detail_proto.accel_type
    INNER JOIN clouddb.organizations o ON o.id = s.org_id
    AND o.internal_type <> 1
    AND a.event_type in (
      'haAccel',
      'haBraking',
      'haCameraMisaligned',
      'haCrash',
      'haDistractedDriving',
      'haDriverObstructionPolicy',
      'haDrowsinessDetection',
      'haLaneDeparture',
      'haNearCollision',
      'haOutwardObstructionPolicy',
      'haPhonePolicy',
      'haSeatbeltPolicy',
      'haSharpTurn',
      'haTailgating',
      'haTileRollingStopSign'
      -- add another event_type here
    )
    AND element_at(s.additional_labels.additional_labels, 1).label_source <> 3 -- ignore manually created events as they will skew the metric massively
  WHERE DATE(s.created_at) >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
  GROUP by
    1,2
)
SELECT
  date,
  event_type,
  p50_latency_mins,
  latency_threshold_mins
FROM latency
WHERE p50_latency_mins > latency_threshold_mins
