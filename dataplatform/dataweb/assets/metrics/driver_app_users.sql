SELECT
  d.date,
  d.org_id,
  d.driver_id,
  last_mobile_login_date,
  first_mobile_login_date,
  sam_map.sam_number,
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
FROM datamodel_platform.dim_drivers d
JOIN datamodel_core.dim_organizations o
  ON d.org_id = o.org_id
  AND d.date = o.date
LEFT JOIN product_analytics.map_org_sam_number_latest sam_map
  ON sam_map.org_id = o.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE is_deleted = FALSE
