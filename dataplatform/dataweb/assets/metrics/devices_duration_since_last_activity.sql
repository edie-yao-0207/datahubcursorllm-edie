WITH duration AS (
  SELECT
    date,
    org_id,
    device_type,
    region,
    device_id,
    p.name AS product_name,
    p.product_id AS product_id,
    dag.asset_type,
    dag.asset_type_name,
    last_activity_date,
    DATEDIFF(dag.date, last_activity_date) AS days_since_last_activity
  FROM dataengineering.device_dormancy_global dag
  JOIN definitions.products p
    ON dag.product_id = p.product_id
  WHERE (
    (
        dag.device_type = 'AG - Asset Gateway'
        AND dag.asset_type NOT IN (0, 3)
        AND dag.asset_type IS NOT NULL
    )
    OR dag.device_type = 'VG - Vehicle Gateway'
    OR p.product_id IN (56, 57, 58, 103, 141, 187) -- 56: OEM Device, 57: OEM Powered Equipment, 58: OEM Vehicle, 103: OEM Reefer, 141: App-based Telematics Device, 187: OEM Trailer
  )
)
SELECT
  d.date,
  d.org_id,
  sam_map.sam_number,
  device_type,
  d.region,
  device_id,
  product_name,
  product_id,
  asset_type,
  asset_type_name,
  last_activity_date,
  days_since_last_activity,
  c.internal_type,
  c.account_size_segment,
  c.industry_vertical_raw AS account_industry,
  c.account_arr_segment,
  c.avg_mileage,
  c.region AS subregion,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  c.is_paid_customer,
  c.is_paid_safety_customer,
  c.is_paid_stce_customer,
  c.is_paid_telematics_customer
FROM duration d
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories_global c
  ON d.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON sam_map.org_id = d.org_id
