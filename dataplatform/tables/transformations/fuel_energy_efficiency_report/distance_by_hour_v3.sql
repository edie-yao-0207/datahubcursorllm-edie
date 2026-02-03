WITH filter_segments AS (
  SELECT *
  FROM fuel_energy_efficiency_report.ecu_filter_segments
  WHERE date >= ${start_date} AND date < ${end_date}
   AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
),

distance_with_ecu_segments AS (
  SELECT
    dist.*,
    seg.segment_start
  FROM canonical_distance.total_distance dist
  LEFT JOIN filter_segments seg
    ON dist.org_id = seg.org_id
    AND dist.device_id = seg.device_id
    AND dist.date = seg.date
    AND dist.time_ms > seg.segment_start and dist.time_ms <= seg.segment_end
  WHERE dist.date >= ${start_date}
  AND dist.date < ${end_date}
  AND dist.org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
),

driver_assignments AS (
  SELECT *
  FROM fuel_energy_efficiency_report.driver_assignments
  WHERE date >= ${start_date}
   AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
),

-- Group canonical distance by（device, driver, hour) and filter out overlapping spotty ECU distance
grouped_distance AS (
  SELECT
    DATE(cd.date) AS date,
    cd.org_id,
    cd.device_id AS object_id,
    COALESCE(driver.driver_id, 0) AS driver_id,
    DATE_TRUNC('hour', FROM_UNIXTIME(time_ms / 1000)) AS interval_start,
    DATE_TRUNC('hour', FROM_UNIXTIME(time_ms / 1000)) + interval 1 hour AS interval_end,
    -- filter out any distance that overlaps with an ECU filter segment
    SUM(IF(segment_start IS NOT NULL, 0, distance)) AS distance_traveled_m
  FROM distance_with_ecu_segments cd
  LEFT JOIN driver_assignments AS driver
    ON driver.org_id = cd.org_id
    AND driver.device_id = cd.device_id
    -- driver assignments can span multiple days, but they will always start on or after the date of the canonical distance they overlap
    AND cd.date >= driver.date
    AND cd.time_ms BETWEEN driver.start_time AND driver.end_time - 1
  WHERE cd.date >= ${start_date}
    AND cd.date < ${end_date}
  GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT *
FROM grouped_distance
WHERE distance_traveled_m IS NOT NULL AND distance_traveled_m != 0
