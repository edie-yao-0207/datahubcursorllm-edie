WITH
behavior_enum_generic_distraction AS (
  SELECT *
  FROM definitions.behavior_label_type_enums
  WHERE behavior_label_type = 'GenericDistraction'
),
behavior_enum_edge_distracted_driving AS (
  SELECT *
  FROM definitions.behavior_label_type_enums
  WHERE behavior_label_type = 'EdgeDistractedDriving'
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
    date,
    device_id,
    org_id,
    associated_devices
  FROM datamodel_core.dim_devices
  WHERE device_type = 'VG - Vehicle Gateway'
),
devices_settings AS (
  SELECT
    date,
    vg_device_id,
    org_id
  FROM product_analytics.dim_devices_safety_settings
  WHERE inattentive_driving_enabled = TRUE
),
inattentive_driving_events AS (
  SELECT
    date,
    DATE_TRUNC('HOUR',FROM_UNIXTIME(trip_start_ms/1000.0)) as interval_start,
    org_id,
    device_id,
    driver_id,
    trip_start_ms,
    trip_end_ms,
    trip_data.duration_ms AS driving_time_ms,
    COALESCE(trip_data.distance_meters,0) AS distance_meters,
    CASE WHEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_generic_distraction.enum)) > 0
        THEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_generic_distraction.enum))
        ELSE 0 END AS generic_distraction_count,
    CASE WHEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_edge_distracted_driving.enum)) > 0
        THEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_edge_distracted_driving.enum))
        ELSE 0 END AS edge_distracted_driving_count
  FROM scoringdb_shards.trip_scores
  LEFT JOIN behavior_enum_generic_distraction ON 1=1
  LEFT JOIN behavior_enum_edge_distracted_driving ON 1=1
)
SELECT
  i.date,
  i.org_id,
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
  i.driver_id,
  i.interval_start,
  i.generic_distraction_count,
  i.edge_distracted_driving_count,
  i.generic_distraction_count + i.edge_distracted_driving_count AS total_distraction_count,
  i.distance_meters * 0.0006213711922 AS distance_miles,
  i.driving_time_ms,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  i.trip_start_ms,
  i.trip_end_ms
FROM dim_devices d
JOIN dim_organizations o
  ON d.org_id = o.org_id
JOIN inattentive_driving_events i
  ON o.org_id = i.org_id
  AND i.device_id = d.device_id
  AND i.date = d.date
JOIN definitions.products p
  ON d.associated_devices.camera_product_id = p.product_id
JOIN devices_settings ds
  ON ds.org_id = d.org_id
  AND ds.vg_device_id = d.device_id
  AND i.date = ds.date
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE p.name IN ('CM32', 'CM34') --only include dual-facing dashcams
