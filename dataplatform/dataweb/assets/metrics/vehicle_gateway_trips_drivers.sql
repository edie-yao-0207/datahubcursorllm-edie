WITH orgs_with_active_licenses AS (
  SELECT DISTINCT sam_number, _run_dt
  FROM edw.silver.fct_license_orders_daily_snapshot dlg
  WHERE
    dlg.net_quantity > 0
),
orgs AS (
  SELECT do.date,
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
  WHERE do.internal_type = 0
  AND do.account_first_purchase_date IS NOT NULL
)
SELECT
  t.date,
  t.org_id,
  o.sam_number,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_telematics_customer,
  o.is_paid_stce_customer,
  driver_id,
  driver_assignment_source,
  trip_type,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM datamodel_telematics.fct_trips t
JOIN orgs o
  ON t.org_id = o.org_id
  AND t.date = o.date
JOIN orgs_with_active_licenses orgs
  ON COALESCE(o.salesforce_sam_number, o.sam_number) = orgs.sam_number
  AND t.date = orgs._run_dt
JOIN datamodel_core.dim_devices dd
  ON t.device_id = dd.device_id
  AND t.org_id = dd.org_id
  AND t.date = dd.date
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE
  dd.device_type = 'VG - Vehicle Gateway'
  AND t.driver_id != 0
