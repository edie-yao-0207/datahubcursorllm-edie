SELECT
  tdg.date,
  tdg.org_id,
  sam_map.sam_number,
  tdg.account_size_segment,
  c.internal_type,
  c.industry_vertical_raw AS account_industry,
  c.account_arr_segment,
  COALESCE(camera_product_name, 'VG Only') AS device_type,
  product_id,
  driver_assignment_source,
  tdg.region,
  NVL2(camera_device_id, 1, 0) AS has_camera,
  IF(num_safety_events > 0, 1, 0) as has_safety_event,
  is_driver_assigned,
  trip_type,
  c.avg_mileage,
  c.region AS subregion,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition,
  c.is_paid_customer,
  c.is_paid_safety_customer,
  c.is_paid_stce_customer,
  c.is_paid_telematics_customer
FROM dataengineering.trip_details_global tdg
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories_global c
  ON tdg.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON sam_map.org_id = tdg.org_id
