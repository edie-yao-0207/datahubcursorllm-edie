WITH
deltas as (
  SELECT
    *
  FROM
    material_usage_report.total_quantity_wet_spread_by_hour
  WHERE
   date >= ${start_date}
    AND date < ${end_date}
),
wet_spread_rate_event_intervals_with_hours AS (
  SELECT
    org_id,
    device_id,
    MODE,
    DATE_TRUNC(
      'hour',
      FROM_UNIXTIME(spread_rate_start_ms / 1000)
    ) AS hour_start,
    spread_rate,
    wet_material_spread_ml,
    time_spread_ms,
    distance_covered_meters
  FROM
    material_usage_report.wet_spread_rate_event_intervals
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),
aggr_wet_material_spread_from_spread_rate AS (
  SELECT
    date(hour_start) as date,
    org_id,
    device_id,
    hour_start AS interval_start,
    sum(wet_material_spread_ml) AS wet_material_spread_ml,
    sum(time_spread_ms) AS duration_spread_ms,
    sum(distance_covered_meters) AS distance_covered_meters
  FROM
    wet_spread_rate_event_intervals_with_hours
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
  deltas.total_quantity_spread as material_spread_ml_from_deltas,
  spread_rate.wet_material_spread_ml as material_spread_ml_from_spread_rate,
  spread_rate.distance_covered_meters as distance_spread_meters_from_spread_rate,
  spread_rate.duration_spread_ms as duration_spread_ms_from_spread_rate
FROM
  deltas
  FULL OUTER JOIN aggr_wet_material_spread_from_spread_rate spread_rate ON deltas.org_id = spread_rate.org_id
  and deltas.device_id = spread_rate.device_id
  and deltas.hour_start = spread_rate.interval_start
