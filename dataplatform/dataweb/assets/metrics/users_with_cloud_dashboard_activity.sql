WITH org_categories AS (
    SELECT
      ocg.org_id,
      sam_map.sam_number,
      sam_map.sam_number AS parent_sam_number,
      ocg.internal_type,
      ocg.avg_mileage,
      ocg.region,
      ocg.fleet_size,
      ocg.industry_vertical,
      ocg.fuel_category,
      ocg.primary_driving_environment,
      ocg.fleet_composition
    FROM product_analytics_staging.stg_organization_categories_global ocg
    LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
      ON sam_map.org_id = ocg.org_id
    WHERE ocg.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
)
SELECT
  date,
  user_email,
  o.parent_sam_number,
  o.sam_number,
  o.internal_type,
  c.org_id,
  c.region,
  o.avg_mileage,
  o.region AS subregion,
  o.fleet_size,
  o.industry_vertical,
  o.fuel_category,
  o.primary_driving_environment,
  o.fleet_composition,
  account_arr_segment,
  account_size_segment_name,
  account_industry,
  is_paid_customer,
  is_paid_safety_customer,
  is_paid_stce_customer,
  is_paid_telematics_customer
FROM dataengineering.cloud_dashboard_user_activity_global c
LEFT OUTER JOIN org_categories o
  ON c.org_id = o.org_id
