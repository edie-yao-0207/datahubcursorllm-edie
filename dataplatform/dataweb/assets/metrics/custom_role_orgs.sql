WITH orgs AS (
  SELECT
    do.date,
    do.org_id,
    do.internal_type,
    sam_map.sam_number,
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
user_roles AS (
  SELECT
    date,
    user_id,
    org_id,
    custom_role_uuids,
    CASE
        WHEN ARRAY_SIZE(custom_role_uuids) > 0 THEN 1
        ELSE 0
    END AS is_custom_role,
    ARRAY_SIZE(custom_role_uuids) AS custom_roles_size
    FROM datamodel_platform.dim_users_organizations
    WHERE
      login_count_30d IS NOT NULL  -- User logged in the last month
      AND login_count_30d > 0
      AND is_samsara_email = FALSE
      AND internal_type = 0
)
SELECT
  user_roles.date,
  user_roles.org_id,
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
  c.fleet_composition,
  COUNT(DISTINCT user_roles.user_id) AS user_count,
  AVG(is_custom_role) AS fraction_custom_role
FROM user_roles
JOIN orgs o
  ON user_roles.date = o.date
  AND user_roles.org_id = o.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
GROUP BY ALL
