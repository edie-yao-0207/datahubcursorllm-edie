SELECT
  mpg.date,
  mpg.org_id,
  sam_map.sam_number,
  device_id,
  lookback_window,
  IF(weighted_mpg between 2 and 200, weighted_mpg, NULL) AS mpge,
  total_fuel_consumed_gallons,
  total_distance_traveled_miles,
  total_energy_consumed_kwh,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM dataengineering.vehicle_mpg_lookback mpg
JOIN datamodel_core.dim_organizations o
  ON mpg.org_id = o.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
LEFT JOIN product_analytics.map_org_sam_number_latest sam_map
  ON sam_map.org_id = mpg.org_id
WHERE o.date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
