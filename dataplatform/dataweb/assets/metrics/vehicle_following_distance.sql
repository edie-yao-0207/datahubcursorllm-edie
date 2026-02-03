WITH behavior_enum_following_distance AS (
  SELECT *
  FROM definitions.behavior_label_type_enums
  WHERE behavior_label_type = 'FollowingDistance'
),
behavior_enum_following_distance_severe AS (
  SELECT *
  FROM definitions.behavior_label_type_enums
  WHERE behavior_label_type = 'FollowingDistanceSevere'
),
behavior_enum_following_distance_moderate AS (
  SELECT *
  FROM definitions.behavior_label_type_enums
  WHERE behavior_label_type = 'FollowingDistanceModerate'
),
dim_organizations AS (
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
  WHERE following_distance_enabled = TRUE
),
following_distance_events AS (
  SELECT
    date,
    DATE_TRUNC('HOUR', FROM_UNIXTIME(trip_start_ms / 1000.0)) AS interval_start,
    org_id,
    device_id,
    driver_id,
    trip_start_ms,
    trip_end_ms,
    trip_data.duration_ms AS driving_time_ms,
    COALESCE(trip_data.distance_meters,0) AS distance_meters,
    CASE WHEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_following_distance.enum)) > 0
        THEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_following_distance.enum))
        ELSE 0 END AS following_distance_count,
    CASE WHEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_following_distance_severe.enum)) > 0
        THEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_following_distance_severe.enum))
        ELSE 0 END AS following_distance_severe_count,
    CASE WHEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_following_distance_moderate.enum)) > 0
        THEN SIZE(FILTER(TRANSFORM(triage_events.event_data.behaviors, x -> x[0]), x -> x.behavior_label = behavior_enum_following_distance_moderate.enum))
        ELSE 0 END AS following_distance_moderate_count
  FROM scoringdb_shards.trip_scores
  LEFT JOIN behavior_enum_following_distance ON 1=1
  LEFT JOIN behavior_enum_following_distance_severe ON 1=1
  LEFT JOIN behavior_enum_following_distance_moderate ON 1=1
)
SELECT
  f.date,
  f.org_id,
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
  f.interval_start,
  f.driving_time_ms,
  f.distance_meters * 0.0006213711922 AS distance_miles,
  f.following_distance_count,
  f.following_distance_severe_count,
  f.following_distance_moderate_count,
  f.following_distance_count + f.following_distance_severe_count + f.following_distance_moderate_count AS following_distance_event_count,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  f.trip_start_ms,
  f.trip_end_ms
FROM dim_devices d
JOIN dim_organizations o
  ON d.org_id = o.org_id
  AND d.date = o.date
JOIN following_distance_events f
  ON o.org_id = f.org_id
  AND f.device_id = d.device_id
  AND f.date = d.date
JOIN definitions.products p
  ON d.associated_devices.camera_product_id = p.product_id
JOIN devices_settings ds
  ON ds.org_id = d.org_id
  AND ds.vg_device_id = d.device_id
  AND f.date = ds.date
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON o.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
WHERE p.name IN ('CM31', 'CM32', 'CM33', 'CM34')
