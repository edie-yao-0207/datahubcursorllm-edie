WITH cloud_toggles AS (
  SELECT
    o.id AS org_id,
    sf.sam_number,
    MAX(CAST(gamification_enabled AS BOOLEAN)) AS gamification_enabled,

    -- if on, ensure parent settings are also on to override a bug in source logging
    MAX(IFNULL(settings_proto.organization_driver_app_settings.customizable_driver_features_settings.eco_driving_score_enabled, false)
        AND IFNULL(settings_proto.organization_driver_app_settings.customizable_driver_features_settings.driver_scores_enabled, false)) AS eco_driving_score_enabled,
    MAX(IFNULL(settings_proto.organization_driver_app_settings.customizable_driver_features_settings.safety_score_ranking_enabled, false)
        AND IFNULL(settings_proto.organization_driver_app_settings.customizable_driver_features_settings.driver_scores_enabled, false)
        AND CAST(gamification_enabled AS BOOLEAN)) AS safety_score_ranking_enabled,
    MAX(IFNULL(settings_proto.forward_collision_warning_enabled, false)) AS forward_collision_warning_enabled,
    MAX(IFNULL(settings_proto.distracted_driving_detection_enabled, false)) AS distracted_driving_detection_enabled,
    MAX(IFNULL(settings_proto.following_distance_enabled, false)) AS following_distance_enabled,
    MAX(IFNULL(settings_proto.adas_all_features_allowed, false)) AS adas_all_features_allowed,
    MAX(IFNULL(settings_proto.safety.coaching_settings.safety_event_auto_triage_enabled, false)) AS safety_event_auto_triage_enabled,
    MAX(CAST(voice_coaching_enabled AS BOOLEAN)) AS voice_coaching_enabled,
    MAX(CAST(canada_hos_enabled AS BOOLEAN)) AS canada_hos_enabled
  FROM clouddb.organizations o
  LEFT JOIN clouddb.groups g ON o.id = g.organization_id
  LEFT JOIN clouddb.org_sfdc_accounts osf ON o.id = osf.org_id
  LEFT JOIN clouddb.sfdc_accounts sf ON osf.sfdc_account_id = sf.id
  GROUP BY
    o.id,
    sf.sam_number
),

org_safety_score AS (
  SELECT
    s.org_id,
    sf.sam_number,
    MAX_BY(s.detail_proto.configurable_score_on, s.created_at) AS safety_score_configured
  FROM safetydb_shards.safety_score_settings s
  LEFT JOIN clouddb.org_sfdc_accounts osf ON s.org_id = osf.org_id
  LEFT JOIN clouddb.sfdc_accounts sf ON osf.sfdc_account_id = sf.id
  GROUP BY
    s.org_id,
    sf.sam_number
),

dashboard_users AS (
    SELECT
      uo.organization_id AS org_id,
      COUNT(DISTINCT u.email_lower) AS total_dashboard_users
    FROM clouddb.users u
    JOIN clouddb.users_organizations uo
      ON uo.user_id = u.id
      AND u.service_account_id IS NULL
    GROUP BY uo.organization_id
)

SELECT
    u.org_id,
    u.total_dashboard_users,
    t.gamification_enabled,
    t.forward_collision_warning_enabled,
    t.distracted_driving_detection_enabled,
    t.following_distance_enabled,
    t.adas_all_features_allowed,
    t.safety_event_auto_triage_enabled,
    t.voice_coaching_enabled,
    t.canada_hos_enabled,
    ss.safety_score_configured,
    t.eco_driving_score_enabled,
    t.safety_score_ranking_enabled
FROM dashboard_users u
LEFT JOIN cloud_toggles t ON
    u.org_id = t.org_id
LEFT JOIN org_safety_score ss ON
  u.org_id = ss.org_id
WHERE u.org_id IS NOT NULL

