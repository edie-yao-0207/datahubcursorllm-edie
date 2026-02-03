SELECT
  d.date,
  d.org_id,
  sam_map.sam_number,
  c.internal_type,
  d.account_size_segment,
  c.industry_vertical_raw AS account_industry,
  c.account_arr_segment,
  device_type,
  d.region,
  device_id,
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
FROM dataengineering.device_activity_global d
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories_global c
  ON d.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON sam_map.org_id = d.org_id
