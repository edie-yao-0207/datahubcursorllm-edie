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
  dims.date,
  dims.org_id,
  orgs.sam_number,
  orgs.internal_type,
  orgs.account_size_segment,
  orgs.account_industry,
  orgs.account_arr_segment,
  orgs.is_paid_customer,
  orgs.is_paid_safety_customer,
  orgs.is_paid_telematics_customer,
  orgs.is_paid_stce_customer,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  dims.device_id,
  dims.product_id,
  dims.make AS vehicle_make,
  dims.model AS vehicle_model,
  dims.year AS vehicle_year,
  dims.engine_model AS vehicle_engine_model,
  dims.primary_fuel_type AS vehicle_primary_fuel_type,
  data.type,
  data.is_covered,
  data.value
FROM product_analytics.dim_device_dimensions AS dims
JOIN product_analytics.agg_device_stats_secondary_coverage AS data
  ON data.date = dims.date
  AND data.org_id = dims.org_id
  AND data.device_id = dims.device_id
JOIN orgs
  ON orgs.date = data.date
  AND orgs.org_id = data.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON orgs.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE dims.diagnostics_capable
  AND dims.include_in_agg_metrics
