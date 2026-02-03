WITH active_devices AS (
  SELECT
    date,
    org_id,
    region,
    COUNT(DISTINCT CASE WHEN is_device_active = 1 THEN device_id END) AS total_active_devices
  FROM dataengineering.device_activity_global
  GROUP BY 1, 2, 3
)
SELECT
  CAST(o.date AS STRING) date,
  o.org_id,
  sam_map.sam_number,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.internal_type,
  o.region,
  COALESCE(ad.total_active_devices, 0) AS active_device_count,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM dataengineering.dim_organizations_global o
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
    ON sam_map.org_id = o.org_id
LEFT OUTER JOIN active_devices ad
    ON ad.org_id = o.org_id
    AND ad.date = o.date
    AND ad.region = o.region
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories_global c
    ON o.org_id = c.org_id
    AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
WHERE
    o.is_paid_customer = TRUE
    AND (
        o.is_paid_safety_customer = TRUE
        OR o.is_paid_stce_customer = TRUE
        OR o.is_paid_telematics_customer = TRUE
    )
