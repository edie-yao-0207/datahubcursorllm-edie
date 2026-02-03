WITH dim_organizations AS (
  SELECT
    do.org_id,
    do.account_arr_segment,
    do.account_industry,
    sam_map.sam_number,
    do.internal_type,
    do.account_size_segment,
    do.is_paid_customer,
    do.is_paid_safety_customer,
    do.is_paid_telematics_customer,
    do.is_paid_stce_customer
  FROM datamodel_core.dim_organizations do
  LEFT JOIN product_analytics.map_org_sam_number_latest sam_map
    ON sam_map.org_id = do.org_id
  WHERE do.date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
)
SELECT
  utilization.date,
  utilization.org_id,
  dim_organizations.sam_number,
  dim_organizations.internal_type,
  dim_organizations.account_size_segment,
  dim_organizations.account_industry,
  dim_organizations.account_arr_segment,
  dim_organizations.is_paid_customer,
  dim_organizations.is_paid_safety_customer,
  dim_organizations.is_paid_telematics_customer,
  dim_organizations.is_paid_stce_customer,
  utilization.device_id,
  utilization.total_available_hours,
  utilization.total_utilized_hours,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM product_analytics_staging.fct_device_utilization utilization
LEFT OUTER JOIN dim_organizations
  ON dim_organizations.org_id = utilization.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON utilization.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
