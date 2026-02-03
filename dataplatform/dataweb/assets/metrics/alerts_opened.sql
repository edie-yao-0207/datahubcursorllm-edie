WITH alert_org_join AS (
  SELECT
    adg.date,
    adg.org_id,
    adg.region,
    EXPLODE(ARRAYS_ZIP(adg.trigger_types, adg.alert_action_types)) AS trigger_type,
    adg.alert_config_id,
    adg.alert_occurred_at_ms,
    adg.mp_processing_time_ms,
    CONCAT(adg.alert_config_id, '_', adg.alert_occurred_at_ms) AS alert_id
  FROM datamodel_platform.fct_alert_details_global adg
),
org_categories AS (
  SELECT
    org_id,
    internal_type,
    avg_mileage,
    region,
    fleet_size,
    industry_vertical,
    industry_vertical_raw AS account_industry,
    fuel_category,
    primary_driving_environment,
    fleet_composition,
    account_size_segment,
    account_arr_segment,
    is_paid_customer,
    is_paid_safety_customer,
    is_paid_telematics_customer,
    is_paid_stce_customer,
    locale
  FROM product_analytics_staging.stg_organization_categories_global
  WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
)
SELECT
  date,
  a.org_id,
  sam_map.sam_number,
  a.region,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.locale,
  trigger_type.trigger_types AS trigger_type,
  trigger_type.alert_action_types AS alert_action_type,
  alert_config_id,
  alert_occurred_at_ms,
  mp_processing_time_ms,
  alert_id,
  o.region AS subregion,
  o.avg_mileage,
  o.fleet_size,
  o.industry_vertical,
  o.fuel_category,
  o.primary_driving_environment,
  o.fleet_composition,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer
FROM alert_org_join a
LEFT OUTER JOIN org_categories o
  ON a.org_id = o.org_id
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON sam_map.org_id = a.org_id
