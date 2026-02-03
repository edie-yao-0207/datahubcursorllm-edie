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
drivers AS (
  SELECT DISTINCT
    d.date,
    d.org_id,
    d.driver_id
  FROM datamodel_platform.dim_drivers d
  JOIN orgs o
    ON d.date = o.date
    AND d.org_id = o.org_id
  WHERE DATEDIFF(d.date, d.last_mobile_login_date) < 30 -- filter to drivers that have logged in in the last month
),
driver_attributes AS (
  WITH attributes AS (
    SELECT
      uuid AS attribute_id,
      name AS attribute_name,
      CAST(created_at AS DATE) AS attribute_created_at,
      CAST(updated_at AS DATE) AS attribute_updated_at
    FROM attributedb_shards.attributes
    WHERE entity_type = 1 -- Only drivers
  ),
  driver_attributes AS (
    SELECT
      entity_id AS driver_id,
      attribute_id,
      CAST(created_at AS DATE) AS attribute_assigned_at
    FROM attributedb_shards.attribute_cloud_entities AS ace
    WHERE entity_type = 1 -- Only drivers
  ),
  attribute_values AS (
    SELECT
      attribute_id,
      COUNT(*) AS attribute_values_n
    FROM attributedb_shards.attribute_values
    GROUP BY attribute_id
  )
  SELECT
    da.driver_id,
    COLLECT_SET(da.attribute_id) AS attribute_ids,
    COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
    COLLECT_SET(a.attribute_name) AS attribute_names
  FROM attributes a
  LEFT OUTER JOIN driver_attributes da
    ON a.attribute_id = da.attribute_id
  LEFT OUTER JOIN attribute_values av
    ON a.attribute_id = av.attribute_id
  WHERE da.driver_id IS NOT NULL
  GROUP BY da.driver_id
)
SELECT
  d.date,
  o.org_id,
  o.sam_number,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  d.driver_id,
  COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM drivers d
LEFT OUTER JOIN orgs o
  ON d.date = o.date
  AND d.org_id = o.org_id
LEFT OUTER JOIN driver_attributes a
  ON d.driver_id = a.driver_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE d.driver_id IS NOT NULL
