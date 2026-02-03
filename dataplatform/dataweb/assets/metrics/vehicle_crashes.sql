WITH
behavior_enum AS (
  SELECT array_agg(DISTINCT enum) AS enum
  FROM definitions.behavior_label_type_enums
  WHERE behavior_label_type = 'Crash'
),
dim_organizations AS (
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
    device_id,
    org_id
  FROM datamodel_core.dim_devices
  WHERE
    device_type = 'VG - Vehicle Gateway'
    AND associated_devices.camera_device_id IS NOT NULL
    AND date = (SELECT MAX(date) FROM datamodel_core.dim_devices)
),
crashes AS (
    SELECT
    date,
    DATE_TRUNC('HOUR',FROM_UNIXTIME(trip_start_ms/1000.0)) as interval_start,
    org_id,
    device_id,
    driver_id,
    trip_start_ms,
    trip_end_ms,
    COALESCE(trip_data.distance_meters,0) AS distance_meters,
    CASE WHEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> ARRAY_CONTAINS(behavior_enum.enum, x.behavior_label) = TRUE)) > 0
        THEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> ARRAY_CONTAINS(behavior_enum.enum, x.behavior_label) = TRUE))
        ELSE 0 END AS crash_count
  FROM scoringdb_shards.trip_scores
  LEFT JOIN behavior_enum ON 1=1
)
SELECT
  c.date,
  c.org_id,
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
  c.driver_id,
  c.interval_start,
  c.crash_count,
  c.distance_meters * 0.0006213711922 AS distance_miles,
  cat.region AS subregion,
  cat.avg_mileage,
  cat.fleet_size,
  cat.industry_vertical,
  cat.fuel_category,
  cat.primary_driving_environment,
  cat.fleet_composition,
  c.trip_start_ms,
  c.trip_end_ms
FROM dim_devices d
JOIN dim_organizations o
  ON d.org_id = o.org_id
JOIN crashes c
  ON o.org_id = c.org_id
  AND c.device_id = d.device_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories cat
  ON o.org_id = cat.org_id
  AND cat.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
