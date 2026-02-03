WITH orgs AS (
  SELECT
    do.date,
    do.org_id,
    sam_map.sam_number,
    do.internal_type,
    do.account_size_segment,
    do.account_industry,
    do.account_arr_segment,
    do.is_paid_customer,
    do.is_paid_safety_customer,
    do.is_paid_telematics_customer,
    do.is_paid_stce_customer
  FROM datamodel_core.dim_organizations do
  LEFT JOIN product_analytics.map_org_sam_number_latest sam_map
    ON sam_map.org_id = do.org_id
)
SELECT
  d.date,
  d.org_id,
  o.sam_number,
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
  c.fleet_composition,
  d.device_id,
  f.fuel_group
FROM datamodel_core.dim_devices d
JOIN orgs o
  ON d.date = o.date
  AND d.org_id = o.org_id
LEFT OUTER JOIN product_analytics_staging.dim_device_vehicle_properties f
  ON f.org_id = d.org_id
  AND f.device_id = d.device_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE
  f.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
  AND d.device_type = 'VG - Vehicle Gateway'
