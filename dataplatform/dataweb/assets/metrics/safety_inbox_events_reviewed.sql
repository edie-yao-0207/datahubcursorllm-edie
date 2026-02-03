SELECT
  esg.date,
  detection_type,
  esg.org_id,
  sam_map.sam_number,
  c.internal_type,
  esg.account_size_segment,
  esg.account_industry,
  esg.account_arr_segment,
  has_ser_license,
  esg.region,
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
  event_ms,
  c.avg_mileage,
  c.region AS subregion,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  c.is_paid_customer,
  c.is_paid_safety_customer,
  c.is_paid_stce_customer,
  c.is_paid_telematics_customer
FROM dataengineering.safety_inbox_events_status_global esg
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories_global c
  ON esg.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON esg.org_id = sam_map.org_id
