WITH distance_diffs AS (
  SELECT *
  FROM objectstat_diffs.gps
  WHERE date >= DATE_SUB(${start_date}, 2)
  AND date < ${end_date}
),

fuel_diffs AS (
  SELECT *
  FROM objectstat_diffs.fuel
  WHERE date >= DATE_SUB(${start_date}, 2)
  AND date < ${end_date}
),

visits AS (
  SELECT *
  FROM time_on_site_report.site_visit_hours
  WHERE date >= DATE_SUB(${start_date}, 2)
  AND date < ${end_date}
)

SELECT
  v.org_id,
  v.device_id,
  v.date,
  address_id,
  interval_start,
  interval_end,
  visit_start_ms,
  visit_end_ms,
  start_ms,
  end_ms,
  SUM(distance_traveled_m) AS distance_m,
  SUM(fuel_consumed_ml) AS fuel_consumed_ml
FROM visits v
LEFT JOIN distance_diffs d
  ON v.org_id = d.org_id
  AND v.device_id = d.object_id
  AND d.time >= v.start_ms
  AND d.time >= v.visit_start_ms
  AND d.time < v.end_ms
  AND d.time < v.visit_end_ms
LEFT JOIN fuel_diffs f
  ON v.org_id = f.org_id
  AND v.device_id = f.object_id
  AND f.time >= v.start_ms
  AND f.time >= v.visit_start_ms
  AND f.time < v.end_ms
  AND f.time < v.visit_end_ms
GROUP BY
  v.org_id,
  v.device_id,
  v.date,
  address_id,
  interval_start,
  interval_end,
  visit_start_ms,
  visit_end_ms,
  start_ms,
  end_ms
HAVING v.date >= ${start_date}
