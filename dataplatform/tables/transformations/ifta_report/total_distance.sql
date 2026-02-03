WITH distance_by_device_jurisdiction_hour AS (
  SELECT
    date,
    WINDOW(FROM_UNIXTIME(start_ms / 1000), "1 hour").start AS hour_start,
    als.org_id,
    als.device_id,
    start_value.jurisdiction,
    CASE WHEN oem.id IS NOT NULL -- use the canonical distance calculated from odometer's values becasue canonical_distance_meters's values used to be calculated incorrectly for OEM devices.
      THEN SUM(COALESCE(end_odo_meters - start_odo_meters, 0))
      ELSE
    SUM(COALESCE(canonical_distance_meters, 0)) END AS canonical_distance_meters,
    SUM(end_gps_meters - start_gps_meters) AS gps_meters,
    SUM(end_odo_meters - start_odo_meters) AS odo_meters,
    SUM(IF(start_value.toll, COALESCE(canonical_distance_meters, 0), 0)) AS canonical_toll_meters,
    SUM(IF(start_value.toll, end_gps_meters - start_gps_meters, 0)) AS gps_toll_meters,
    SUM(IF(start_value.toll, end_odo_meters - start_odo_meters, 0)) AS odo_toll_meters,
    SUM(IF(start_value.off_highway, COALESCE(canonical_distance_meters, 0.0), 0.0)) AS canonical_off_highway_meters,
    SUM(IF(start_value.private_road, COALESCE(canonical_distance_meters, 0.0), 0.0)) AS canonical_private_road_meters
  FROM ifta_report.annotated_location_segments als
    LEFT JOIN productsdb.devices oem ON als.org_id = oem.org_id
    AND als.device_id = oem.id
    AND oem.product_id=58
  WHERE date >= ${start_date} AND date < ${end_date}
    AND UPPER(start_value.revgeo_country) IN ('US', 'CA', 'MX') -- only aggregate jurisdictions for IFTA countries.
  GROUP BY date, als.org_id, als.device_id, oem.id, start_value.jurisdiction, WINDOW
)

SELECT *, CURRENT_TIMESTAMP() AS updated_at FROM distance_by_device_jurisdiction_hour
