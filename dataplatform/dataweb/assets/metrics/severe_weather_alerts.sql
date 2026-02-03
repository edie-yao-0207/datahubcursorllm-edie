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
  DATE(t.created_at) AS date,
  t.org_id,
  o.sam_number,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  t.uuid AS trigger_uuid,
  i.name AS incident_name,
  CONCAT(i.object_ids, '_', i.occurred_at_ms, '_', i.org_id, '_', i.workflow_id) AS workflow_incident_id,
  t.proto_data.severe_weather_alert.severities AS severe_weather_data,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM workflowsdb_shards.triggers t
JOIN workflowsdb_shards.workflow_incidents i
  ON t.workflow_uuid = i.workflow_id
JOIN orgs o
  ON t.org_id = o.org_id
  AND DATE(t.created_at) = o.date
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE
  t.type = 5036 -- Severe Weather
  AND (t.deleted_at IS NULL OR DATE(t.deleted_at) >= DATE(t.created_at))
  AND i.occurred_at_ms > (UNIX_TIMESTAMP(t.created_at) * 1000)
