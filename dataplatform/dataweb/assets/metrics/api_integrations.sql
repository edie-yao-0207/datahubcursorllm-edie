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
),
integrations AS (
  SELECT
    dai.date,
    dai.org_id,
    'OAuth' AS type,
    dai.oauth_token_app_uuid AS token_id
  FROM product_analytics.fct_api_usage dai
  WHERE oauth_token_app_uuid != ''
      AND oauth_token_app_uuid IS NOT NULL

  UNION

  SELECT
    dai.date,
    dai.org_id,
    'API' AS type,
    dai.api_token_id AS token_id
  FROM product_analytics.fct_api_usage dai
  WHERE (
    oauth_token_app_uuid = ''
    OR oauth_token_app_uuid IS NULL
  )
)
SELECT
  i.date,
  i.org_id,
  i.type,
  i.token_id,
  o.sam_number,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_telematics_customer,
  o.is_paid_stce_customer,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM integrations i
JOIN orgs o
  ON i.org_id = o.org_id
  AND i.date = o.date
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
