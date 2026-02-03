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
    date = (SELECT MAX(date) from datamodel_platform.dim_users)
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
  DATE(FROM_UNIXTIME(d.created_at_ms / 1000)) AS date,
  d.org_id,
  orgs.sam_number,
  orgs.internal_type,
  orgs.account_size_segment,
  orgs.account_industry,
  orgs.account_arr_segment,
  orgs.is_paid_customer,
  orgs.is_paid_safety_customer,
  orgs.is_paid_telematics_customer,
  orgs.is_paid_stce_customer,
  d.uuid,
  d.title,
  cr.content_import_status,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  CASE WHEN e.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_internal
FROM trainingdb_shards.courses d
INNER JOIN orgs
  ON d.org_id = orgs.org_id
JOIN trainingdb_shards.course_revisions cr
  ON d.current_revision_uuid = cr.uuid
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON orgs.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
LEFT OUTER JOIN internal_users e
  ON d.created_by = e.user_id
WHERE TRUE
  AND d.global_course_uuid IS NULL
  AND COALESCE(DATE(FROM_UNIXTIME(d.deleted_at_ms / 1000)), DATE_ADD(DATE(FROM_UNIXTIME(d.created_at_ms / 1000)), 1)) >= DATE(FROM_UNIXTIME(d.created_at_ms / 1000))
  AND COALESCE(DATE(FROM_UNIXTIME(cr.deleted_at_ms / 1000)), DATE_ADD(DATE(FROM_UNIXTIME(d.created_at_ms / 1000)), 1)) >= DATE(FROM_UNIXTIME(d.created_at_ms / 1000))
