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
  vml.date,
  g.organization_id AS org_id,
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
  vml.vehicle_id,
  vml.id
FROM clouddb.vehicle_maintenance_logs vml
JOIN clouddb.groups g
  ON vml.group_id = g.id
LEFT OUTER JOIN orgs o
  ON vml.date = o.date
  AND g.organization_id = o.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON g.organization_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE (vml.deleted_at IS NULL OR DATE(vml.deleted_at) >= vml.date OR DATE(vml.deleted_at) = '0101-01-01')
