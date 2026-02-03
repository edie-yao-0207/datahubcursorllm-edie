WITH org_categories AS (
  SELECT
    ocg.org_id,
    sam_map.sam_number,
    ocg.org_name,
    ocg.locale,
    ocg.internal_type,
    ocg.avg_mileage,
    ocg.region,
    ocg.fleet_size,
    ocg.industry_vertical_raw AS account_industry,
    ocg.industry_vertical,
    ocg.fuel_category,
    ocg.primary_driving_environment,
    ocg.fleet_composition,
    ocg.account_size_segment,
    ocg.account_arr_segment,
    ocg.is_paid_customer,
    ocg.is_paid_safety_customer,
    ocg.is_paid_stce_customer,
    ocg.is_paid_telematics_customer
  FROM product_analytics_staging.stg_organization_categories_global ocg
  LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
    ON ocg.org_id = sam_map.org_id
  WHERE ocg.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
)
SELECT
  date,
  detection_type,
  s.org_id,
  o.org_name,
  o.locale,
  o.sam_number,
  o.internal_type,
  s.account_size_segment,
  s.account_industry,
  s.account_arr_segment,
  has_ser_license,
  s.region,
  o.avg_mileage,
  o.region AS subregion,
  o.fleet_size,
  o.industry_vertical,
  o.fuel_category,
  o.primary_driving_environment,
  o.fleet_composition,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  is_coaching_assigned,
  is_car_viewed,
  is_customer_dismissed,
  is_viewed,
  is_useful,
  is_actioned,
  is_critical_event,
  is_auto_coached,
  has_safety_event_review,
  is_coached,
  device_id,
  event_ms
FROM dataengineering.safety_inbox_events_status_global s
LEFT OUTER JOIN org_categories o
  ON s.org_id = o.org_id
