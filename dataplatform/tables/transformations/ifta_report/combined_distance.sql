-- Transform the imported v1 data into the same format as total_distance (v2)
WITH historical_ifta_v2 AS (
  SELECT
    -- Use TO_DATE since the delta table stores timestamps
    TO_DATE(date) as date,
    CAST(FROM_UNIXTIME((end_ms-MAX(duration_ms))/1000) AS TIMESTAMP) AS interval_start,
    org_id,
    device_id,
    COALESCE(jurisdiction, "") as jurisdiction,
    IF(
      MAX(use_tripless),
      IF(MAX(use_odometer), SUM(tripless_odo_distance_m), SUM(tripless_gps_distance_m)),
      IF(MAX(use_odometer), SUM(odo_distance_m), SUM(gps_distance_m))
    ) AS distance_meters,
    IF(MAX(use_tripless), SUM(tripless_gps_distance_m), SUM(gps_distance_m)) AS gps_meters,
    IF(MAX(use_tripless), SUM(tripless_odo_distance_m), SUM(odo_distance_m)) AS odo_meters,
    IF(MAX(use_tripless), SUM(tripless_toll_distance_m), SUM(toll_distance_m)) AS toll_meters,
    0.0 AS off_highway_meters,
    0.0 AS private_road_meters
  FROM fuel_maintenance.historical_ifta_report_delta
  GROUP BY org_id, device_id, jurisdiction, date, end_ms
),

-- Combine v1 and v2 data only for the switchover day
-- Centuri (58287) opted for v2 data only, so ignore it for the switchover day, and pull all its v2 data in the final union
combined_dist_switchover_day AS (
  -- v2 data
  SELECT
    date,
    hour_start AS interval_start,
    org_id,
    device_id,
    jurisdiction,
    false AS historical,
    DOUBLE(canonical_distance_meters) AS total_distance_meters,
    DOUBLE(gps_meters) AS gps_meters,
    DOUBLE(odo_meters) AS odo_meters,
    DOUBLE(canonical_toll_meters) AS toll_meters,
    DOUBLE(canonical_off_highway_meters) AS off_highway_meters,
    DOUBLE(canonical_private_road_meters) AS private_road_meters
  FROM ifta_report.total_distance
  WHERE date >= ${start_date}
    AND date < ${end_date}
    AND date = TO_DATE("2021-03-01")
    AND org_id <> 58287
  -- v1 - Precomputed in the old reports framework and imported to Spark
  UNION
  SELECT
    date,
    interval_start,
    org_id,
    device_id,
    jurisdiction,
    true AS historical,
    DOUBLE(distance_meters) AS total_distance_meters,
    DOUBLE(gps_meters) AS gps_meters,
    DOUBLE(odo_meters) AS odo_meters,
    DOUBLE(toll_meters) AS toll_meters,
    0.0 AS off_highway_meters,
    0.0 AS private_road_meters
  FROM historical_ifta_v2
  WHERE date >= ${start_date}
    AND date < ${end_date}
    AND date = TO_DATE("2021-03-01")
    AND org_id <> 58287
),

-- Join the switchover day data with org timezones and calculate the ms offset from UTC
switchover_day_with_offset AS (
  SELECT
    combined_dist_switchover_day.*,
    UNIX_TIMESTAMP(MAKE_TIMESTAMP(2021,3,1,0,0,0))*1000 - UNIX_TIMESTAMP(MAKE_TIMESTAMP(2021,3,1,0,0,0,timezone))*1000 as ms_offset
  FROM combined_dist_switchover_day LEFT JOIN clouddb.groups groups ON combined_dist_switchover_day.org_id = groups.organization_id
  WHERE date = TO_DATE("2021-03-01")
 )


-- v1 - Precomputed in the old reports framework and imported to Spark
SELECT
  date,
  interval_start,
  org_id,
  device_id,
  jurisdiction,
  true AS historical,
  DOUBLE(distance_meters) AS total_distance_meters,
  DOUBLE(gps_meters) AS gps_meters,
  DOUBLE(odo_meters) AS odo_meters,
  DOUBLE(toll_meters) AS toll_meters,
  0.0 AS off_highway_meters,
  0.0 AS private_road_meters
FROM historical_ifta_v2
WHERE date >= ${start_date}
  AND date < ${end_date}
  AND date < TO_DATE("2021-03-01") -- Show v1 data before the switchover date
  AND org_id <> 58287     -- Centuri opted out of v1 data
-- v1 and v2 filtered mileage on the switchover day
UNION
SELECT
  date,
  interval_start,
  org_id,
  device_id,
  jurisdiction,
  historical,
  total_distance_meters,
  gps_meters,
  odo_meters,
  toll_meters,
  off_highway_meters,
  private_road_meters
FROM switchover_day_with_offset
-- Split the switchover day based on each org's timezone offset from UTC. Subtract an extra hour because we are using interval starts
-- Select historical data before the switchover, and non-historical data after the switchover
WHERE (interval_start <= CAST(FROM_UNIXTIME((UNIX_TIMESTAMP(MAKE_TIMESTAMP(2021,3,1,0,0,0))*1000 - ms_offset - 3600000)/1000) AS TIMESTAMP) AND historical = true)
  OR (interval_start > CAST(FROM_UNIXTIME((UNIX_TIMESTAMP(MAKE_TIMESTAMP(2021,3,1,0,0,0))*1000 - ms_offset - 3600000)/1000) AS TIMESTAMP) AND historical = false)
  AND date = TO_DATE("2021-03-01")
  AND org_id <> 58287
-- v2 - New computed mileage based on canonical distance after the switchover
UNION
SELECT
  date,
  hour_start AS interval_start,
  org_id,
  device_id,
  jurisdiction,
  false AS historical,
  DOUBLE(canonical_distance_meters) AS total_distance_meters,
  DOUBLE(gps_meters) AS gps_meters,
  DOUBLE(odo_meters) AS odo_meters,
  -- Toll meters should not be > total meters, and if total meters is negative, make toll meters 0
  DOUBLE(IF(canonical_toll_meters > canonical_distance_meters, IF(canonical_distance_meters < 0, 0, canonical_distance_meters), canonical_toll_meters)) AS toll_meters,
  DOUBLE(canonical_off_highway_meters) AS off_highway_meters,
  DOUBLE(canonical_private_road_meters) AS private_road_meters
FROM ifta_report.total_distance
WHERE date >= ${start_date}
  AND date < ${end_date}
  AND (
    date > TO_DATE("2021-03-01")    -- Only show v2 data after the switchover date
    OR org_id = 58287      -- Centuri opted for v2 data only, so pull all v2 data regardless of switchover
  )
  AND (
    date,
    org_id,
    hour_start,
    device_id,
    jurisdiction
  ) NOT IN (
    SELECT
      date,
      org_id,
      interval_start,
      device_id,
      jurisdiction
    FROM fuel_maintenance.ifta_report_combined_distance_filtered_rows
  )
