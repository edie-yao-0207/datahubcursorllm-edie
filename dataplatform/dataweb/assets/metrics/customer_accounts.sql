WITH org_categories AS (
  SELECT
    org_id,
    internal_type,
    avg_mileage,
    region,
    fleet_size,
    account_size_segment,
    industry_vertical_raw AS account_industry,
    industry_vertical,
    fuel_category,
    primary_driving_environment,
    fleet_composition,
    account_arr_segment,
    is_paid_customer,
    is_paid_safety_customer,
    is_paid_stce_customer,
    is_paid_telematics_customer
  FROM product_analytics_staging.stg_organization_categories_global
  WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
)
SELECT
  date,
  d.org_id,
  sam_map.sam_number,
  o.internal_type,
  d.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  product_id,
  device_type,
  d.region,
  o.avg_mileage,
  o.region AS subregion,
  o.fleet_size,
  o.industry_vertical,
  o.fuel_category,
  o.primary_driving_environment,
  o.fleet_composition,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer
FROM dataengineering.device_activity_global d
LEFT OUTER JOIN org_categories o
  ON d.org_id = o.org_id
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON sam_map.org_id = d.org_id
WHERE
  is_device_online = 1
