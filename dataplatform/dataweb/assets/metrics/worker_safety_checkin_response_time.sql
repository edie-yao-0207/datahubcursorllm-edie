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
  t.date,
  t.orgid AS org_id,
  o.sam_number,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  t.timeruuid,
  t.status AS timer_status,
  t.response AS timer_response,
  t.recipientpolymorphicuserid AS recipient_user_id,
  t.startat AS timer_start_at,
  t.recipientacknowledgedat AS recipient_acknowledged_at,
  CASE WHEN i.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_internal,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM dynamodb.worker_safety_timers t
JOIN orgs o
  ON t.orgid = o.org_id
  AND t.date = o.date
LEFT OUTER JOIN internal_users i
  ON SPLIT_PART(t.recipientpolymorphicuserid, '-', 2) = i.user_id
  AND SPLIT_PART(t.recipientpolymorphicuserid, '-', 1) = 'user'
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE t.RecipientAcknowledgedAt IS NOT NULL
