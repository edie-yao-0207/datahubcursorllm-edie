WITH
deltas as (
  SELECT
    *
  FROM
    material_usage_report.total_quantity_dry_spread_by_hour
  WHERE
   date >= ${start_date}
    AND date < ${end_date}
),
dry_spread_rate_event_intervals_with_hours AS (
  SELECT
    org_id,
    device_id,
    MODE,
    DATE_TRUNC(
      'hour',
      FROM_UNIXTIME(spread_rate_start_ms / 1000)
    ) AS hour_start,
    spread_rate,
    dry_material_spread_mg,
    time_spread_ms,
    distance_covered_meters
  FROM
    material_usage_report.dry_spread_rate_event_intervals
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),
aggr_dry_material_spread_from_spread_rate AS (
  SELECT
    date(hour_start) as date,
    org_id,
    device_id,
    hour_start AS interval_start,
    sum(dry_material_spread_mg) AS dry_material_spread_mg,
    sum(time_spread_ms) AS duration_spread_ms,
    sum(distance_covered_meters) AS distance_covered_meters
  FROM
    dry_spread_rate_event_intervals_with_hours
  GROUP BY
    date,
    org_id,
    device_id,
    hour_start
)
SELECT
  coalesce(deltas.date, spread_rate.date) as date,
  coalesce(deltas.org_id, spread_rate.org_id) as org_id,
  coalesce(
    deltas.device_id,
    spread_rate.device_id
  ) as device_id,
  coalesce(
    deltas.hour_start,
    spread_rate.interval_start
  ) as interval_start,
  deltas.total_quantity_spread as material_spread_mg_from_deltas,
  spread_rate.dry_material_spread_mg as material_spread_mg_from_spread_rate,
  spread_rate.distance_covered_meters as distance_spread_meters_from_spread_rate,
  spread_rate.duration_spread_ms as duration_spread_ms_from_spread_rate
FROM
  deltas
  FULL OUTER JOIN aggr_dry_material_spread_from_spread_rate spread_rate ON deltas.org_id = spread_rate.org_id
  and deltas.device_id = spread_rate.device_id
  and deltas.hour_start = spread_rate.interval_start
