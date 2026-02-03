SELECT
  a.date,
  a.org_id,
  sam_map.sam_number,
  a.is_admin,
  a.primary_trigger_type,
  a.secondary_trigger_type,
  alert_action_type,
  alert_recipient_type,
  alert_notification_type,
  created_ts_utc,
  alert_config_uuid,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_telematics_customer,
  o.is_paid_stce_customer,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM datamodel_platform.dim_alert_configs a
JOIN datamodel_core.dim_organizations o
  ON a.date = o.date
  AND a.org_id = o.org_id
LEFT JOIN product_analytics.map_org_sam_number_latest sam_map
  ON sam_map.org_id = o.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
LATERAL VIEW EXPLODE(
    CASE WHEN a.alert_action_types IS NULL OR SIZE(a.alert_action_types) = 0 THEN ARRAY(NULL) ELSE a.alert_action_types END
) AS alert_action_type
LATERAL VIEW EXPLODE(
    CASE WHEN a.alert_recipient_types IS NULL OR SIZE(a.alert_recipient_types) = 0 THEN ARRAY(NULL) ELSE a.alert_recipient_types END
) AS alert_recipient_type
LATERAL VIEW EXPLODE(
    CASE WHEN a.alert_notification_types IS NULL OR SIZE(a.alert_notification_types) = 0 THEN ARRAY(NULL) ELSE a.alert_notification_types END
) AS alert_notification_type
WHERE
  is_disabled = FALSE
  AND (COALESCE(DATE(deleted_ts_utc), DATE_ADD(a.date, 1)) > a.date)
