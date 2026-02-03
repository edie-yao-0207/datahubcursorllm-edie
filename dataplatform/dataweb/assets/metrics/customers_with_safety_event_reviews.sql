WITH org_categories AS (
  SELECT
    org_id,
    internal_type,
    avg_mileage,
    region,
    fleet_size,
    industry_vertical_raw AS account_industry,
    industry_vertical,
    fuel_category,
    primary_driving_environment,
    fleet_composition,
    account_size_segment,
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
  s.region,
  o.internal_type,
  o.avg_mileage,
  o.region AS subregion,
  o.fleet_size,
  o.industry_vertical,
  o.fuel_category,
  o.primary_driving_environment,
  o.fleet_composition,
  s.org_id,
  s.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  sam_map.sam_number,
  has_ser_license,
  NVL2(completed_at, 1, 0) AS has_review_completed,
  detection_type,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer
FROM dataengineering.safety_event_reviews_details s
LEFT OUTER JOIN org_categories o
  ON s.org_id = o.org_id
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON sam_map.org_id = s.org_id
