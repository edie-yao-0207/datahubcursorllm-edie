WITH dim_organizations AS (
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
dim_devices AS (
  SELECT
    date,
    device_id,
    org_id
  FROM datamodel_core.dim_devices
  WHERE device_type = 'VG - Vehicle Gateway'
),
speeding AS (
  SELECT
    date,
    DATE_TRUNC('HOUR', FROM_UNIXTIME(trip_start_ms / 1000.0)) as interval_start,
    org_id,
    device_id,
    driver_id,
    trip_start_ms,
    trip_end_ms,
    trip_data.speeding_durations.severe_ms AS severe_speeding_ms,
    trip_data.duration_ms AS driving_time_ms
  FROM scoringdb_shards.trip_scores
)
SELECT
  s.date,
  s.org_id,
  o.sam_number,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_telematics_customer,
  o.is_paid_stce_customer,
  d.device_id,
  s.driver_id,
  s.interval_start,
  s.severe_speeding_ms,
  s.driving_time_ms,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  s.trip_start_ms,
  s.trip_end_ms
FROM dim_devices d
JOIN dim_organizations o
  ON d.org_id = o.org_id
JOIN speeding s
  ON o.org_id = s.org_id
  AND s.device_id = d.device_id
  AND d.date = s.date
JOIN product_analytics.dim_devices_safety_settings ss
  ON d.org_id = ss.org_id
  AND s.date = ss.date
  AND ss.vg_device_id = s.device_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE ss.severe_speeding_enabled
