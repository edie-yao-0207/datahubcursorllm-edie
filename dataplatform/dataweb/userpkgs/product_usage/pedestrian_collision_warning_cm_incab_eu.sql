WITH all_roadside_settings_eu AS (
  SELECT
    EntityId,
    Namespace,
    FROM_JSON(Settings, 'struct<vulnerableRoadUserCollisionWarningSettings:struct<audioAlerts:struct<enabled:boolean>, enabled:boolean, enabledCameraStreams:struct<front:boolean, left:boolean, rear:boolean, right:boolean, roadFacingDashcam:boolean>>>').vulnerableRoadUserCollisionWarningSettings.audioAlerts.enabled AS enabled,
    _approximate_creation_date_time
  FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/settings`
  WHERE
    Namespace LIKE 'SafetyEventDetection%'
    AND EntityId LIKE 'ORG%'
),
enabled_history_eu AS (
  SELECT
    EntityId,
    _approximate_creation_date_time,
    ROW_NUMBER() OVER (PARTITION BY EntityId ORDER BY _approximate_creation_date_time) AS rn
  FROM all_roadside_settings_eu
  WHERE enabled = true
),
first_enabled_eu AS (
  SELECT
    EntityId,
    MIN(_approximate_creation_date_time) AS enabled_at
  FROM enabled_history_eu
  GROUP BY EntityId
),
current_settings_eu AS (
  SELECT
    EntityId,
    _approximate_creation_date_time AS last_updated
  FROM all_roadside_settings_eu
  WHERE
    Namespace = 'SafetyEventDetection'
    AND enabled = true
)
SELECT DISTINCT
    REGEXP_EXTRACT(cs.EntityId, 'ORG#(.+)', 1) AS org_id,
    FROM_UNIXTIME(fe.enabled_at / 1000, 'yyyy-MM-dd HH:mm:ss') AS date
FROM current_settings_eu cs
JOIN first_enabled_eu fe
  ON cs.EntityId = fe.EntityId
