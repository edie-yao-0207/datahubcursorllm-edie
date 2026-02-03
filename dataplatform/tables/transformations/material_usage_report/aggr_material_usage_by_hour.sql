WITH aggr_dry_material_spread AS (
  SELECT
    *
  FROM
    material_usage_report.aggr_dry_material_spread_by_hour
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),
aggr_wet_material_spread AS (
  SELECT
    *
  FROM
    material_usage_report.aggr_wet_material_spread_by_hour
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),
aggr_prewet_material_spread AS (
  SELECT
    *
  FROM
    material_usage_report.aggr_prewet_material_spread_by_hour
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),
aggr_material_usage AS (
  SELECT
    COALESCE(dry.date, wet.date, prewet.date) as date,
    COALESCE(dry.org_id, wet.org_id, prewet.org_id) as org_id,
    COALESCE(dry.device_id, wet.device_id, prewet.device_id) as device_id,
    COALESCE(
      dry.interval_start,
      wet.interval_start,
      prewet.interval_start
    ) as interval_start,
    dry.material_spread_mg_from_deltas as dry_quantity_spread_mg_from_deltas,
    dry.material_spread_mg_from_spread_rate as dry_quantity_spread_mg_from_spread_rate,
    dry.distance_spread_meters_from_spread_rate as dry_distance_spread_meters_from_spread_rate,
    dry.duration_spread_ms_from_spread_rate as dry_duration_spread_ms_from_spread_rate,
    wet.material_spread_ml_from_deltas as wet_quantity_spread_ml_from_deltas,
    wet.material_spread_ml_from_spread_rate as wet_quantity_spread_ml_from_spread_rate,
    wet.distance_spread_meters_from_spread_rate as wet_distance_spread_meters_from_spread_rate,
    wet.duration_spread_ms_from_spread_rate as wet_duration_spread_ms_from_spread_rate,
    prewet.material_spread_ml_from_deltas as prewet_quantity_spread_ml_from_deltas,
    prewet.material_spread_ml_from_spread_rate as prewet_quantity_spread_ml_from_spread_rate,
    prewet.distance_spread_meters_from_spread_rate as prewet_distance_spread_meters_from_spread_rate,
    prewet.duration_spread_ms_from_spread_rate as prewet_duration_spread_ms_from_spread_rate,
    prewet.material_spread_ml_from_mix_rate as prewet_quantity_spread_ml_from_mix_rate,
    prewet.distance_spread_meters_from_mix_rate as prewet_distance_spread_meters_from_mix_rate,
    prewet.duration_spread_ms_from_mix_rate as prewet_duration_spread_ms_from_mix_rate
  FROM
    aggr_dry_material_spread dry
    FULL OUTER JOIN aggr_wet_material_spread wet
    ON dry.date = wet.date
      AND dry.org_id = wet.org_id
      AND dry.device_id = wet.device_id
      AND dry.interval_start = wet.interval_start
    FULL OUTER JOIN aggr_prewet_material_spread prewet
    ON (
      (dry.date = prewet.date
      AND dry.org_id = prewet.org_id
      AND dry.device_id = prewet.device_id
      AND dry.interval_start = prewet.interval_start)
    OR (prewet.date = wet.date
      AND prewet.org_id = wet.org_id
      AND prewet.device_id = wet.device_id
      AND prewet.interval_start = wet.interval_start)
    )
),
time_driven AS (
  SELECT
    date,
    org_id,
    device_id,
    hour_start AS interval_start,
    duration_driven_ms
  FROM
    material_usage_report.time_driven_by_hour
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),
aggr_material_usage_with_time_driven AS (
  SELECT
    material.*,
    duration_driven_ms
  FROM
    aggr_material_usage material
    left join time_driven on material.date = time_driven.date
    and material.org_id = time_driven.org_id
    and material.device_id = time_driven.device_id
    and material.interval_start = time_driven.interval_start
)
SELECT
  date(interval_start) as date,
  org_id,
  device_id,
  interval_start,
  dry_quantity_spread_mg_from_deltas,
  dry_quantity_spread_mg_from_spread_rate,
  dry_distance_spread_meters_from_spread_rate,
  dry_duration_spread_ms_from_spread_rate,
  wet_quantity_spread_ml_from_deltas,
  wet_quantity_spread_ml_from_spread_rate,
  wet_distance_spread_meters_from_spread_rate,
  wet_duration_spread_ms_from_spread_rate,
  prewet_quantity_spread_ml_from_deltas,
  prewet_quantity_spread_ml_from_spread_rate,
  prewet_distance_spread_meters_from_spread_rate,
  prewet_duration_spread_ms_from_spread_rate,
  prewet_quantity_spread_ml_from_mix_rate,
  prewet_distance_spread_meters_from_mix_rate,
  prewet_duration_spread_ms_from_mix_rate,
  duration_driven_ms
FROM
  aggr_material_usage_with_time_driven
