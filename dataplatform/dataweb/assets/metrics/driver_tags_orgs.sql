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
driver_tags AS (
  SELECT
    driver_id,
    COLLECT_SET(tag_id) AS tag_ids
  FROM clouddb.tag_drivers AS td
  LEFT OUTER JOIN clouddb.tags AS t
    ON t.id = td.tag_id
  GROUP BY driver_id
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
  COALESCE(ARRAY_SIZE(t.tag_ids), 0) AS tags_size,
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
LEFT OUTER JOIN driver_tags t
  ON d.driver_id = t.driver_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE d.driver_id IS NOT NULL
