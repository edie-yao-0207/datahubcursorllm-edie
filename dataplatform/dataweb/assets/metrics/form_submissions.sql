WITH orgs AS (
  SELECT
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
  WHERE do.date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
),
internal_users AS (
  SELECT user_id
  FROM datamodel_platform.dim_users
  WHERE
    date = (SELECT MAX(date) FROM datamodel_platform.dim_users)
    AND (
        is_samsara_email = TRUE
        OR email LIKE '%samsara.canary%'
        OR email LIKE '%samsara.forms.canary%'
        OR email LIKE '%samsaracanarydevcontractor%'
        OR email LIKE '%samsaratest%'
        OR email LIKE '%@samsara%'
        OR email LIKE '%@samsara-service-account.com'
    )
)
SELECT
  DATE(a.server_updated_at) AS date,
  a.org_id,
  orgs.sam_number,
  orgs.internal_type,
  orgs.account_size_segment,
  orgs.account_industry,
  orgs.account_arr_segment,
  orgs.is_paid_customer,
  orgs.is_paid_safety_customer,
  orgs.is_paid_telematics_customer,
  orgs.is_paid_stce_customer,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  a.uuid,
  a.assigned_at,
  a.started_at,
  a.completed_at,
  SPLIT_PART(a.assigned_to_polymorphic, '-', 1) AS user_category,
  SPLIT_PART(a.assigned_to_polymorphic, '-', 2) AS user_id,
  a.status,
  CASE WHEN e.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_internal
FROM formsdb_shards.form_submissions a
INNER JOIN orgs
  ON a.org_id = orgs.org_id
LEFT JOIN internal_users e
  ON SPLIT_PART(a.submitted_by_polymorphic, '-', 2) = e.user_id
  AND SPLIT_PART(a.submitted_by_polymorphic, '-', 1) = 'user'
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON orgs.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE COALESCE(DATE(a.server_deleted_at), DATE_ADD(DATE(a.server_updated_at), 1)) >= DATE(a.server_updated_at)
