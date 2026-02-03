WITH all_roadside_settings_us AS (
  SELECT
    EntityId,
    Namespace,
    FROM_JSON(Settings, 'struct<vulnerableRoadUserCollisionWarningSettings:struct<audioAlerts:struct<enabled:boolean>, enabled:boolean, enabledCameraStreams:struct<front:boolean, left:boolean, rear:boolean, right:boolean, roadFacingDashcam:boolean>>>').vulnerableRoadUserCollisionWarningSettings.audioAlerts.enabled AS enabled,
    _approximate_creation_date_time
  FROM dynamodb.settings
  WHERE
    Namespace LIKE 'SafetyEventDetection%'
    AND EntityId LIKE 'ORG%'
),
enabled_history_us AS (
  SELECT
    EntityId,
    _approximate_creation_date_time,
    ROW_NUMBER() OVER (PARTITION BY EntityId ORDER BY _approximate_creation_date_time) AS rn
  FROM all_roadside_settings_us
  WHERE enabled = true
),
first_enabled_us AS (
  SELECT
    EntityId,
    MIN(_approximate_creation_date_time) AS enabled_at
  FROM enabled_history_us
  GROUP BY EntityId
),
current_settings_us AS (
  SELECT
    EntityId,
    _approximate_creation_date_time AS last_updated
  FROM all_roadside_settings_us
  WHERE
    Namespace = 'SafetyEventDetection'
    AND enabled = true
)
SELECT DISTINCT
    REGEXP_EXTRACT(cs.EntityId, 'ORG#(.+)', 1) AS org_id,
    FROM_UNIXTIME(fe.enabled_at / 1000, 'yyyy-MM-dd HH:mm:ss') AS date
FROM current_settings_us cs
JOIN first_enabled_us fe
  ON cs.EntityId = fe.EntityId
