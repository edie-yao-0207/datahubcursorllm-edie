WITH total_distance AS (
  SELECT *
  FROM canonical_distance.total_distance
  WHERE date >= ${start_date}
    AND date < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
),

annotated_location_segments AS (
  SELECT *
  FROM ifta_report.annotated_location_segments
  WHERE date >= ${start_date}
    AND date < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
)

SELECT
  org_id,
  date,
  device_id,
  time_ms,
  distance,
  interpolated,
  osd_odometer
FROM (
  SELECT
    loc.start_ms,
    cd.*
  FROM total_distance AS cd
  LEFT JOIN annotated_location_segments AS loc
    ON cd.time_ms >= loc.start_ms AND cd.time_ms < loc.end_ms
    AND cd.date = loc.date
    AND cd.org_id = loc.org_id
    AND cd.device_id = loc.device_id
)
WHERE start_ms IS NULL
