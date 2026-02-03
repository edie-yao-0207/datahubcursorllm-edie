WITH orgs_with_active_licenses AS (
  SELECT DISTINCT _run_dt, sam_number
  FROM edw.silver.fct_license_orders_daily_snapshot dlg
  WHERE dlg.net_quantity > 0
)
SELECT
  tdg.date,
  tdg.org_id,
  sam_map.sam_number,
  tdg.region,
  c.internal_type,
  tdg.account_size_segment,
  c.industry_vertical_raw AS account_industry,
  c.account_arr_segment,
  device_type,
  COALESCE(camera_product_name, 'VG Only') AS attached_device_type,
  is_driver_assigned,
  driver_assignment_source,
  IF(num_safety_events > 0, 1, 0) AS has_safety_event,
  NVL2(camera_device_id, 1, 0) AS has_camera,
  distance_miles,
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
JOIN orgs_with_active_licenses orgs
  ON orgs.sam_number = COALESCE(c.salesforce_sam_number, c.sam_number)
  AND tdg.date = orgs._run_dt
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON tdg.org_id = sam_map.org_id
