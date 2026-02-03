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
active_devices AS (
  SELECT
    lda.date,
    lda.org_id,
    lda.device_id
  FROM datamodel_core.lifetime_device_activity AS lda
  JOIN datamodel_core.dim_devices d
    ON d.device_id = lda.device_id
    AND lda.org_id = d.org_id
    AND lda.date = d.date
  JOIN orgs o
    ON lda.org_id = o.org_id
    AND lda.date = o.date
  WHERE DATE_DIFF(lda.date, lda.latest_date) < 365  -- Filter to devices that have been active in the past year
),
device_tags AS (
  SELECT
    device_id,
    COLLECT_SET(tag_id) AS tag_ids
  FROM clouddb.tag_devices AS td
  LEFT OUTER JOIN clouddb.tags AS t
    ON t.id = td.tag_id
  GROUP BY device_id
),
device_attributes AS (
  WITH attributes AS (
    SELECT
      uuid AS attribute_id,
      name AS attribute_name,
      CAST(created_at AS DATE) AS attribute_created_at,
      CAST(updated_at AS DATE) AS attribute_updated_at
    FROM attributedb_shards.attributes
    WHERE entity_type = 2 -- Only vehicles
  ),
  vehicle_attributes AS (
    SELECT
      entity_id AS device_id,
      attribute_id,
      CAST(created_at AS DATE) AS attribute_assigned_at
    FROM attributedb_shards.attribute_cloud_entities AS ace
    WHERE entity_type = 2 -- Only vehicles
  ),
  attribute_values AS (
    SELECT
      attribute_id,
      COUNT(*) AS attribute_values_n
    FROM attributedb_shards.attribute_values
    GROUP BY attribute_id
  )
  SELECT
    va.device_id,
    COLLECT_SET(va.attribute_id) AS attribute_ids,
    COLLECT_SET(av.attribute_values_n) AS attribute_values_n,
    COLLECT_SET(a.attribute_name) AS attribute_names
  FROM attributes a
  LEFT OUTER JOIN vehicle_attributes va
    ON a.attribute_id = va.attribute_id
  LEFT OUTER JOIN attribute_values av
    ON a.attribute_id = av.attribute_id
  GROUP BY va.device_id
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
  d.device_id,
  COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
  COALESCE(ARRAY_SIZE(a.attribute_ids), 0) AS attributes_size,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM active_devices d
LEFT OUTER JOIN orgs o
  ON d.date = o.date
  AND d.org_id = o.org_id
LEFT OUTER JOIN device_tags t
  ON d.device_id = t.device_id
LEFT OUTER JOIN device_attributes a
  ON d.device_id = a.device_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
