SELECT
  csg.date,
  csg.org_id,
  sam_map.sam_number,
  csg.region,
  NVL2(session_completed_time, 1, 0) as is_session_completed,
  session_started_time,
  id,
  c.internal_type,
  c.account_size_segment,
  c.industry_vertical_raw AS account_industry,
  c.account_arr_segment,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  c.is_paid_customer,
  c.is_paid_safety_customer,
  c.is_paid_stce_customer,
  c.is_paid_telematics_customer
FROM dataengineering.safety_event_coaching_sessions_global csg
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON sam_map.org_id = csg.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories_global c
  ON csg.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
