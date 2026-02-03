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
    WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
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
  CAST(DATE(c._timestamp) AS STRING) as date,
  c.org_id,
  o.internal_type,
  o.sam_number,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  c.uuid,
  c.total_rows_errored,
  c.status,
  cat.region AS subregion,
  cat.avg_mileage,
  cat.fleet_size,
  cat.industry_vertical,
  cat.fuel_category,
  cat.primary_driving_environment,
  cat.fleet_composition,
  CASE WHEN e.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_internal
FROM csvuploadsdb_shards.csv_uploads c
JOIN orgs o
  ON c.org_id = o.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories cat
  ON o.org_id = cat.org_id
  AND cat.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
LEFT OUTER JOIN internal_users e
  ON c.created_by = e.user_id
WHERE total_rows_submitted > 0
