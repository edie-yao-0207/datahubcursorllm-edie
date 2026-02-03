WITH orgs AS (
  SELECT
    do.date,
    do.org_id,
    sam_map.sam_number,
    do.salesforce_sam_number,
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
  l.date,
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
  l.sku,
  l.uuid,
  l.entity_type
FROM datamodel_platform.dim_license_assignment l
JOIN orgs o
  ON l.date = o.date
  AND l.org_id = o.org_id
JOIN edw.silver.fct_license_orders_daily_snapshot lic
  ON l.sku = lic.product_sku
  AND COALESCE(o.salesforce_sam_number, o.sam_number) = lic.sam_number
  AND l.date = CAST(lic._run_dt AS STRING)
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE
  COALESCE(l.end_time, l.date) >= l.date
  AND lic.net_quantity > 0
  AND l.is_expired = FALSE
