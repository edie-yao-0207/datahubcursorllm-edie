WITH org_categories AS (
  SELECT
    ocg.org_id,
    sam_map.sam_number,
    ocg.internal_type,
    ocg.avg_mileage,
    ocg.region,
    ocg.fleet_size,
    ocg.industry_vertical_raw AS account_industry,
    ocg.industry_vertical,
    ocg.fuel_category,
    ocg.primary_driving_environment,
    ocg.fleet_composition,
    ocg.account_size_segment,
    ocg.account_arr_segment,
    ocg.is_paid_customer,
    ocg.is_paid_safety_customer,
    ocg.is_paid_stce_customer,
    ocg.is_paid_telematics_customer
  FROM product_analytics_staging.stg_organization_categories_global ocg
  LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
    ON ocg.org_id = sam_map.org_id
  WHERE ocg.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
)
SELECT
  id,
  date,
  v.org_id,
  v.region,
  o.internal_type,
  o.avg_mileage,
  o.region AS subregion,
  o.fleet_size,
  o.industry_vertical,
  o.fuel_category,
  o.primary_driving_environment,
  o.fleet_composition,
  device_type,
  o.sam_number,
  v.account_size_segment,
  v.account_industry,
  v.account_arr_segment,
  is_retrieval_successful,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer
FROM dataengineering.video_requests_global v
LEFT OUTER JOIN org_categories o
  ON v.org_id = o.org_id
