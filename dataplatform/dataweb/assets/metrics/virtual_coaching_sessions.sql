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
)
SELECT
  cs.date,
  cs.org_id,
  o.sam_number,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  cs.uuid,
  cs.completed_date,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM coachingdb_shards.coaching_sessions cs
LEFT OUTER JOIN orgs o
  ON cs.org_id = o.org_id
  AND cs.date = o.date
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON cs.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE session_type = 3 -- session_type 3 is self coaching: https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/safety/coaching/coachingproto/coachingsession.proto
