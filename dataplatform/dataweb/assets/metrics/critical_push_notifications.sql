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
    do.is_paid_stce_customer,
    do.is_paid_telematics_customer
  FROM datamodel_core.dim_organizations do
  LEFT JOIN product_analytics.map_org_sam_number_latest sam_map
    ON sam_map.org_id = do.org_id
)
SELECT
  wi.date,
  wi.org_id,
  o.sam_number,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  wi.workflow_id,
  wi.occurred_at_ms,
  wi.object_ids,
  wi.incident_status,
  wi.severity,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM workflowsdb_shards.workflow_incidents wi
JOIN workflowsdb_shards.actions a
  ON wi.workflow_id = a.workflow_uuid
  AND wi.org_id = a.org_id
JOIN workflowsdb_shards.notification_role_recipients nrr
  ON a.uuid = nrr.action_uuid
  AND a.org_id = nrr.org_id
JOIN orgs o
  ON wi.org_id = o.org_id
  AND wi.date = o.date
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE
  nrr.bypass_dnd = 1
  AND a.type = 1 -- Notification action type
  AND nrr.created_at <= FROM_UNIXTIME(wi.occurred_at_ms / 1000)
  AND (nrr.deleted_at IS NULL OR nrr.deleted_at >= FROM_UNIXTIME(wi.occurred_at_ms / 1000))
