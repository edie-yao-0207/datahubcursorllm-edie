-- Purpose: SQL query to alert when the rate of persisted events for high ACV customers drops to 0.
WITH high_acv_safety_events AS (
  SELECT
    osd.date,
    a.event_type,
    COUNT(DISTINCT osd.value.proto_value.accelerometer_event.event_id) AS total_events,
    COUNT(DISTINCT s.detail_proto.event_id) AS persisted_events,
    ROUND((persisted_events / total_events)*100,1) AS persisted_rate
  FROM kinesisstats.osdaccelerometer osd
  INNER JOIN customer360.customer_metadata csm ON osd.org_id = csm.org_id
  LEFT JOIN safetydb_shards.safety_events s ON s.detail_proto.event_id = osd.value.proto_value.accelerometer_event.event_id
  INNER JOIN definitions.harsh_accel_type_enums a ON a.enum = osd.value.proto_value.accelerometer_event.harsh_accel_type
  WHERE osd.date >= DATEADD(day, -1, CURRENT_TIMESTAMP)
  AND s.date >= DATEADD(day, -1, CURRENT_TIMESTAMP)
  -- csm tiers picked from:
  -- https://samsara.atlassian-us-gov-mod.net/wiki/spaces/CST/pages/17953367/Customer+Success+Tiers
  -- https://samsara.atlassian-us-gov-mod.net/wiki/spaces/Support/pages/17954423/Understanding+CS+Tiers+in+Salesforce
  AND csm.csm_tier in ('Elite', 'Premier', 'Strategic High', 'Select High', 'Core High')
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
      -- add new event type here
  )
  GROUP by 1, 2
  ORDER by 1 ASC
)

SELECT * FROM high_acv_safety_events
WHERE persisted_rate = 0 AND total_events > 1000 -- 1000 is an arbitrary value to reduce noise
