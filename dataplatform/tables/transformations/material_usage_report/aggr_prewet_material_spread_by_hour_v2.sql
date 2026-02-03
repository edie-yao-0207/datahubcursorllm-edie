WITH
deltas as (
  SELECT
    *
  FROM
    material_usage_report.total_quantity_prewet_from_deltas_by_hour
  WHERE
   date >= ${start_date}
    AND date < ${end_date}
),
prewet_spread_rate_with_hours AS (
  SELECT
    org_id,
    device_id,
    mode,
    DATE_TRUNC(
      'hour',
      FROM_UNIXTIME(spread_rate_start_ms / 1000)
    ) as hour_start,
    spread_rate,
    spread_rate_start_ms,
    spread_rate_end_ms,
    distance_covered_meters as distance_spread_meters,
    time_spread_ms as duration_spread_ms,
    prewet_material_spread_ml
  FROM
    material_usage_report.prewet_spread_rate_ml_per_meter_event_intervals_v2
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),
aggr_prewet_material_spread_from_spread_rate as (
  SELECT
    date(hour_start) as date,
    org_id,
    device_id,
    hour_start as interval_start,
    sum(prewet_material_spread_ml) as prewet_material_spread_ml,
    sum(duration_spread_ms) as duration_spread_ms,
    sum(distance_spread_meters) as distance_spread_meters
  FROM
    prewet_spread_rate_with_hours
  GROUP BY
    date,
    org_id,
    device_id,
    hour_start
),
prewet_mix_spread_rate_with_hours AS (
  SELECT
    org_id,
    device_id,
    mode,
    DATE_TRUNC(
      'hour',
      FROM_UNIXTIME(spread_rate_start_ms / 1000)
    ) as hour_start,
    spread_rate,
    spread_rate_start_ms,
    spread_rate_end_ms,
    distance_covered_meters as distance_spread_meters,
    time_spread_ms as duration_spread_ms,
    prewet_material_spread_ml
  FROM
    material_usage_report.prewet_mix_spread_rate_event_intervals_v2
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),
aggr_prewet_material_spread_from_mix_rate as (
  SELECT
    date(hour_start) as date,
    org_id,
    device_id,
    hour_start as interval_start,
    sum(prewet_material_spread_ml) as prewet_material_spread_ml,
    sum(duration_spread_ms) as duration_spread_ms,
    sum(distance_spread_meters) as distance_spread_meters
  FROM
    prewet_mix_spread_rate_with_hours
  GROUP BY
    date,
    org_id,
    device_id,
    hour_start
)
SELECT
  coalesce(deltas.date, spread_rate.date, mix.date) as date,
  coalesce(deltas.org_id, spread_rate.org_id, mix.org_id) as org_id,
  coalesce(
    deltas.device_id,
    spread_rate.device_id,
    mix.device_id
  ) as device_id,
  coalesce(
    deltas.hour_start,
    spread_rate.interval_start,
    mix.interval_start
  ) as interval_start,
  deltas.total_quantity_spread as material_spread_ml_from_deltas,
  spread_rate.prewet_material_spread_ml as material_spread_ml_from_spread_rate,
  spread_rate.distance_spread_meters as distance_spread_meters_from_spread_rate,
  spread_rate.duration_spread_ms as duration_spread_ms_from_spread_rate,
  mix.prewet_material_spread_ml as material_spread_ml_from_mix_rate,
  mix.distance_spread_meters as distance_spread_meters_from_mix_rate,
  mix.duration_spread_ms as duration_spread_ms_from_mix_rate
FROM
  deltas FULL
  OUTER JOIN aggr_prewet_material_spread_from_spread_rate spread_rate ON deltas.org_id = spread_rate.org_id
  and deltas.device_id = spread_rate.device_id
  and deltas.hour_start = spread_rate.interval_start FULL
  OUTER JOIN aggr_prewet_material_spread_from_mix_rate mix ON deltas.org_id = mix.org_id
  and deltas.device_id = mix.device_id
  and deltas.hour_start = mix.interval_start
