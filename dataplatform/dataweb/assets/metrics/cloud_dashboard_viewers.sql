SELECT
  cr.date,
  useremail AS user_email,
  cr.is_customer_email,
  cr.org_id,
  sam_map.sam_number,
  user_id,
  routename,
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
FROM datamodel_platform_silver.stg_cloud_routes cr
LEFT JOIN product_analytics_staging.stg_organization_categories_global c
  ON cr.org_id = c.org_id
  AND c.date = (SELECT MAX(date) from product_analytics_staging.stg_organization_categories_global)
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON sam_map.org_id = cr.org_id
