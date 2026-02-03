WITH org_categories AS (
  SELECT
    org_id,
    org_name,
    locale,
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
  s.org_id,
  sam_map.sam_number,
  o.org_name,
  o.locale,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  has_ser_license,
  s.region,
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
  o.is_paid_telematics_customer,
  detection_type,
  IF(harsh_event_surveys[0].is_useful, 1, 0) AS is_useful,
  is_safety_event_review,
  FORMAT_STRING('%s:%s:%s', s.org_id, device_id, event_ms) AS id
FROM dataengineering.safety_event_surveys_global s
LEFT OUTER JOIN org_categories o
  ON s.org_id = o.org_id
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON sam_map.org_id = s.org_id
