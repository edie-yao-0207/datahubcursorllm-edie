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
  DATE(a.server_created_at) AS date,
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
  a.uuid,
  d.title,
  a.assigned_at,
  a.started_at,
  a.completed_at,
  CASE
    WHEN SPLIT_PART(a.assigned_to_polymorphic, '-', 1) = 'driver'
    THEN SPLIT_PART(a.assigned_to_polymorphic, '-', 2)
    ELSE NULL
  END AS assigned_to_driver,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  CASE WHEN e.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_internal,
  finf.number_value
FROM formsdb_shards.form_submissions a -- find completed trainings
INNER JOIN trainingdb_shards.courses d -- find the name of the training
  ON a.form_template_uuid = d.uuid
INNER JOIN orgs
  ON a.org_id = orgs.org_id
INNER JOIN formsdb_shards.form_index_number_fields finf
  ON finf.form_submission_uuid = a.uuid
LEFT OUTER JOIN internal_users e
  ON SPLIT_PART(a.assigned_to_polymorphic, '-', 2) = e.user_id
  AND SPLIT_PART(a.assigned_to_polymorphic, '-', 1) = 'user'
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON orgs.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE
  a.product_type = 3 -- trainings
  AND COALESCE(DATE(a.server_deleted_at), DATE_ADD(DATE(a.completed_at), 1)) >= DATE(a.completed_at)
  AND a.completed_at IS NOT NULL
  AND finf.field_key = 'timeSpentSeconds'
