 WITH most_recent_result_type AS (
  SELECT
    job_uuid,
    result_type,
    date
  FROM safetyeventreviewdb.review_results -- this finds SER review job results
  QUALIFY RANK() OVER(PARTITION BY date, job_uuid ORDER BY updated_at DESC) = 1 -- get most recent occurrence per job_uuid
),
org_categories AS (
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
  serd.date,
  serd.region,
  o.internal_type,
  o.avg_mileage,
  o.region AS subregion,
  o.fleet_size,
  o.industry_vertical,
  o.fuel_category,
  o.primary_driving_environment,
  o.fleet_composition,
  serd.org_id,
  o.sam_number,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  has_ser_license,
  detection_type,
  NVL2(completed_at, 1, 0) AS is_review_completed,
  CASE -- mark as accepted if the detection type is reviewed as correct
      WHEN coalesce(serd.result_type, mrrt.result_type) != 2 then 1
      ELSE 0
  END AS is_accepted,
  uuid
FROM dataengineering.safety_event_reviews_details serd
LEFT JOIN most_recent_result_type mrrt
  ON serd.date = mrrt.date
  AND serd.uuid = mrrt.job_uuid
LEFT OUTER JOIN org_categories o
  ON serd.org_id = o.org_id
