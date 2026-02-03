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
    ON sam_map.org_id = ocg.org_id
  WHERE ocg.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
)
SELECT
  mr.date,
  COALESCE(mr.org_id, CAST(regexp_extract(mr.mp_current_url, '/o/([0-9]+)/') AS BIGINT)) as org_id,
  org.sam_number,
  CASE WHEN mr.mp_current_url LIKE '%cloud.eu.samsara.com%' THEN 'eu-west-1' WHEN mr.mp_current_url LIKE '%cloud.ca.samsara.com%' THEN 'ca-central-1' ELSE 'us-west-2' END as region,
  mr.distinct_id,
  mr.is_customer_email,
  org.internal_type,
  org.account_size_segment,
  org.account_industry,
  org.account_arr_segment,
  org.avg_mileage,
  org.region AS subregion,
  org.fleet_size,
  org.industry_vertical,
  org.fuel_category,
  org.primary_driving_environment,
  org.fleet_composition,
  org.is_paid_customer,
  org.is_paid_safety_customer,
  org.is_paid_stce_customer,
  org.is_paid_telematics_customer
FROM datamodel_platform_silver.stg_cloud_routes mr -- here we grab cloud route data to extract customers who access Safety related cloud pages
LEFT OUTER JOIN org_categories org
  ON mr.org_id = org.org_id
WHERE
  mr.routename IN ('fleet_safety_inbox', 'fleet_safety_aggregated_inbox') -- Only fleet_safety_inbox is live
GROUP BY ALL
