WITH
  date_table AS (
    SELECT EXPLODE(SEQUENCE(DATE(${start_date}), DATE(${end_date}), INTERVAL 1 DAY)) as date
  ),

  -- events_can_self_coach long
  -- number of triage events where a driver is assigned and the assigned driver has the driver app installed
  event_timeframe AS (
    SELECT
      triage_events_v2.org_id,
      triage_events_v2.driver_id,
      triage_events_v2.behavior_labels,
      date_table.date
    FROM safetyeventtriagedb_shards.triage_events_v2 INNER JOIN date_table
    ON triage_events_v2.start_ms <= unix_millis(timestamp(date_table.date))
    WHERE triage_events_v2.org_id IS NOT NULL
      AND triage_events_v2.driver_id IS NOT NULL
      AND triage_events_v2.driver_id != 0  -- must have driver assigned
  ),
  event_eligible AS (
    SELECT
      event_timeframe.org_id,
      event_timeframe.driver_id,
      event_timeframe.behavior_labels,
      event_timeframe.date
    FROM event_timeframe INNER JOIN risk_overview.driver_filtered_app
    ON event_timeframe.driver_id = driver_filtered_app.id
  ),
  event_self_coach_counts AS (
    SELECT
      org_id,
      'max_speed' AS behavior,
      event_eligible.date,
      COUNT(*) AS ct
      FROM event_eligible
      WHERE behavior_labels LIKE '%|32|%'
      GROUP BY org_id, date
    UNION ALL
    SELECT
      org_id,
      'severe_speeding' AS behavior,
      event_eligible.date,
      COUNT(*) AS ct
      FROM event_eligible
      WHERE behavior_labels like '%|33|%'
      GROUP BY org_id, date
    UNION ALL
    SELECT
      org_id,
      'following_distance' AS behavior,
      event_eligible.date,
      COUNT(*) AS ct
      FROM event_eligible
      WHERE behavior_labels like '%|13|%' OR behavior_labels like '%|26|%'
      GROUP BY org_id, date
    UNION ALL
    SELECT
      org_id,
      'drowsiness' AS behavior,
      event_eligible.date,
      COUNT(*) AS ct
      FROM event_eligible
      WHERE behavior_labels like '%|11|%'
      GROUP BY org_id, date
    UNION ALL
    SELECT
      org_id,
      'mobile_usage' AS behavior,
      event_eligible.date,
      COUNT(*) AS ct
      FROM event_eligible
      WHERE behavior_labels like '%|10|%'
      GROUP BY org_id, date
    UNION ALL
    SELECT
      org_id,
      'inattentive_driving' AS behavior,
      event_eligible.date,
      COUNT(*) AS ct
      FROM event_eligible
      WHERE behavior_labels LIKE '%|2|%' OR behavior_labels like '%|27|%'
      GROUP BY org_id, date
  ),

  -- active_driver_facing_cameras_enabled long
  -- number of active driver-facing cameras with the behavior enabled (device setting)
  device_setting_timeframe AS (
    SELECT
      device_settings.org_id,
      device_settings.vg_device_id,
      date_table.date,
      ANY(device_settings.severe_speeding_enabled) AS severe_speeding_enabled,
      ANY(device_settings.following_distance_enabled) AS following_distance_enabled,
      ANY(device_settings.drowsiness_detection_enabled) AS drowsiness_detection_enabled,
      ANY(device_settings.mobile_usage_enabled) AS mobile_usage_enabled,
      ANY(device_settings.inattentive_driving_enabled) AS inattentive_driving_enabled
    FROM product_analytics.dim_devices_safety_settings AS device_settings INNER JOIN date_table
    ON device_settings.date = date_format(date_table.date, 'yyyy-MM-dd')
    GROUP BY device_settings.org_id, device_settings.vg_device_id, date_table.date
  ),
  camera_driver_facing_active_cols AS (
    SELECT
      camera_driver_facing_active.org_id,
      camera_driver_facing_active.date,
      COUNT(CASE WHEN device_setting_timeframe.severe_speeding_enabled IS TRUE THEN 1 END) AS severe_speeding_cameras,
      COUNT(CASE WHEN device_setting_timeframe.following_distance_enabled IS TRUE THEN 1 END) AS following_distance_cameras,
      COUNT(CASE WHEN device_setting_timeframe.drowsiness_detection_enabled IS TRUE THEN 1 END) AS drowsiness_cameras,
      COUNT(CASE WHEN device_setting_timeframe.mobile_usage_enabled IS TRUE THEN 1 END) AS mobile_usage_cameras,
      COUNT(CASE WHEN device_setting_timeframe.inattentive_driving_enabled IS TRUE THEN 1 END) AS inattentive_driving_cameras
    FROM risk_overview.camera_driver_facing_active LEFT OUTER JOIN device_setting_timeframe
    ON camera_driver_facing_active.id = device_setting_timeframe.vg_device_id
      AND camera_driver_facing_active.org_id = device_setting_timeframe.org_id
      AND camera_driver_facing_active.date = device_setting_timeframe.date
    GROUP BY camera_driver_facing_active.org_id, camera_driver_facing_active.date
  ),

  -- behavior_enabled long
  -- whether behavior is enabled (as an org setting)
  behavior_enabled_booleans AS (
    SELECT
      org_settings.org_id,
      date_table.date,
      ANY(org_settings.max_speed_enabled) as max_speed_enabled,
      ANY(org_settings.severe_speeding_enabled) as severe_speeding_enabled,
      ANY(org_settings.following_distance_enabled) as following_distance_enabled,
      ANY(org_settings.drowsiness_detection_enabled) as drowsiness_detection_enabled,
      ANY(org_settings.mobile_usage_enabled) as mobile_usage_enabled,
      ANY(org_settings.distracted_driving_detection_enabled) as distracted_driving_detection_enabled
    FROM product_analytics.dim_organizations_safety_settings AS org_settings INNER JOIN date_table
      ON org_settings.date = date_format(date_table.date, 'yyyy-MM-dd')
    GROUP BY org_settings.org_id, date_table.date
  ),

  -- combine org-level and behavior-level stats
  org_behavior_cols AS (
    SELECT
      org_stats.org_id,
      org_stats.date,
      org_stats.active_drivers,
      org_stats.active_drivers_with_app,
      org_stats.active_driver_facing_cameras,
      COALESCE(camera_driver_facing_active_cols.severe_speeding_cameras, 0) AS severe_speeding_cameras,
      COALESCE(camera_driver_facing_active_cols.following_distance_cameras, 0) AS following_distance_cameras,
      COALESCE(camera_driver_facing_active_cols.drowsiness_cameras, 0) AS drowsiness_cameras,
      COALESCE(camera_driver_facing_active_cols.mobile_usage_cameras, 0) AS mobile_usage_cameras,
      COALESCE(camera_driver_facing_active_cols.inattentive_driving_cameras, 0) AS inattentive_driving_cameras,
      behavior_enabled_booleans.max_speed_enabled,
      behavior_enabled_booleans.severe_speeding_enabled,
      behavior_enabled_booleans.following_distance_enabled,
      behavior_enabled_booleans.drowsiness_detection_enabled,
      behavior_enabled_booleans.mobile_usage_enabled,
      behavior_enabled_booleans.distracted_driving_detection_enabled
    FROM risk_overview.org_stats
    LEFT OUTER JOIN camera_driver_facing_active_cols
      ON org_stats.org_id = camera_driver_facing_active_cols.org_id
      AND org_stats.date = camera_driver_facing_active_cols.date
    LEFT OUTER JOIN behavior_enabled_booleans
      ON org_stats.org_id = behavior_enabled_booleans.org_id
      AND org_stats.date = behavior_enabled_booleans.date
  ),
  org_behavior_rows AS (
    SELECT
      org_id,
      'max_speed' AS behavior,
      date,
      active_drivers,
      active_drivers_with_app,
      active_driver_facing_cameras,
      NULL AS active_driver_facing_cameras_enabled,  -- max_speed is not configurable as a device-level setting
      max_speed_enabled AS behavior_enabled
    FROM org_behavior_cols
    UNION ALL
    SELECT
      org_id,
      'severe_speeding' AS behavior,
      date,
      active_drivers,
      active_drivers_with_app,
      active_driver_facing_cameras,
      severe_speeding_cameras AS active_driver_facing_cameras_enabled,
      severe_speeding_enabled AS behavior_enabled
    FROM org_behavior_cols
    UNION ALL
      SELECT
      org_id,
      'following_distance' AS behavior,
      date,
      active_drivers,
      active_drivers_with_app,
      active_driver_facing_cameras,
      following_distance_cameras AS active_driver_facing_cameras_enabled,
      following_distance_enabled AS behavior_enabled
    FROM org_behavior_cols
    UNION ALL
    SELECT
      org_id,
      'drowsiness' AS behavior,
      date,
      active_drivers,
      active_drivers_with_app,
      active_driver_facing_cameras,
      drowsiness_cameras AS active_driver_facing_cameras_enabled,
      drowsiness_detection_enabled AS behavior_enabled
    FROM org_behavior_cols
    UNION ALL
    SELECT
      org_id,
      'mobile_usage' AS behavior,
      date,
      active_drivers,
      active_drivers_with_app,
      active_driver_facing_cameras,
      mobile_usage_cameras AS active_driver_facing_cameras_enabled,
      mobile_usage_enabled AS behavior_enabled
    FROM org_behavior_cols
    UNION ALL
    SELECT
      org_id,
      'inattentive_driving' AS behavior,
      date,
      active_drivers,
      active_drivers_with_app,
      active_driver_facing_cameras,
      inattentive_driving_cameras AS active_driver_facing_cameras_enabled,
      distracted_driving_detection_enabled AS behavior_enabled
    FROM org_behavior_cols
  )

SELECT
  DATE_TRUNC('HOUR', org_behavior_rows.date) AS interval_start,
  org_behavior_rows.org_id,
  org_behavior_rows.behavior,
  org_behavior_rows.date,
  org_behavior_rows.active_drivers,
  org_behavior_rows.active_drivers_with_app,
  org_behavior_rows.active_driver_facing_cameras,
  org_behavior_rows.behavior_enabled,
  org_behavior_rows.active_driver_facing_cameras_enabled,
  COALESCE(event_self_coach_counts.ct, 0) AS events_can_self_coach
FROM org_behavior_rows LEFT OUTER JOIN event_self_coach_counts
ON org_behavior_rows.org_id = event_self_coach_counts.org_id
  AND org_behavior_rows.behavior = event_self_coach_counts.behavior
  AND org_behavior_rows.date = event_self_coach_counts.date
