WITH helpers AS (
  SELECT 0.000264172 AS gallon_per_ml
),

-- Do not join with custom_fuel_unit_cost_interval or dynamic_fuel_unit_cost_interval yet.
-- Joins in this view are on the same set of keys (org_id, object_id, driver_id, interval_start).
-- Joins with fuel cost views are on a reduced set of keys (org_id, interval_start) only, which
-- requires colocating data from the same org, which leads to skews.
rows_without_fuel_cost AS (
  SELECT
    COALESCE(esh.date, eh.date, evd.date, rh.date, dh.date) AS date,
    COALESCE(esh.org_id, eh.org_id, evd.org_id, rh.org_id, dh.org_id) AS org_id,
    COALESCE(esh.object_id, eh.object_id, evd.object_id, rh.object_id, dh.object_id) AS object_id,
    COALESCE(esh.driver_id, eh.driver_id, evd.driver_id, rh.driver_id, dh.driver_id, 0) AS driver_id,
    COALESCE(esh.interval_start, eh.interval_start, evd.interval_start, rh.interval_start, dh.interval_start) AS interval_start,
    COALESCE(esh.interval_start, eh.interval_start, evd.interval_start, rh.interval_start, dh.interval_start) + INTERVAL 1 HOURS as interval_end,
    esh.fuel_consumed_ml AS fuel_consumed_ml,
    eh.energy_consumed_kwh AS energy_consumed_kwh,
    dh.distance_traveled_m AS distance_traveled_m,
    uc.cost_per_kwh * eh.energy_consumed_kwh AS dynamic_energy_cost,
    esh.on_duration_ms AS on_duration_ms,
    esh.idle_duration_ms AS idle_duration_ms,
    esh.aux_during_idle_ms AS aux_during_idle_ms,
    -- electric distance traveled is capped by canonical distance traveled
    IF(
      COALESCE(evd.electric_distance_traveled_m_odo, 0) > COALESCE(dh.distance_traveled_m, 0),
      dh.distance_traveled_m,
      DOUBLE(evd.electric_distance_traveled_m_odo)
    ) AS electric_distance_traveled_m,
    rh.energy_regen_kwh AS energy_regen_kwh
  FROM
    fuel_energy_efficiency_report.engine_state_fuel_by_hour_v3 esh
    FULL OUTER JOIN fuel_energy_efficiency_report.energy_consumption_by_hour AS eh
    ON esh.date = eh.date
    AND esh.org_id = eh.org_id
    AND esh.object_id = eh.object_id
    AND esh.driver_id = eh.driver_id
    AND esh.interval_start = eh.interval_start
    FULL OUTER JOIN fuel_energy_efficiency_report.electric_distance_by_hour AS evd
    ON esh.date = evd.date
    AND esh.org_id = evd.org_id
    AND esh.object_id = evd.object_id
    AND esh.driver_id = evd.driver_id
    AND esh.interval_start = evd.interval_start
    FULL OUTER JOIN fuel_energy_efficiency_report.regen_by_hour AS rh
    ON esh.date = rh.date
    AND esh.org_id = rh.org_id
    AND esh.object_id = rh.object_id
    AND esh.driver_id = rh.driver_id
    AND esh.interval_start = rh.interval_start
    FULL OUTER JOIN fuel_energy_efficiency_report.distance_by_hour_v3 AS dh
    ON esh.date = dh.date
    AND esh.org_id = dh.org_id
    AND esh.object_id = dh.object_id
    AND esh.driver_id = dh.driver_id
    AND esh.interval_start = dh.interval_start
  CROSS JOIN helpers.unit_conversion uc
),

-- We have an aggregation step here since the multiple FULL OUTER JOINs above
-- may produce several rows (each with a single non-null value) for each
-- date, org, device, driver, interval_start grouping and we need to consolidate
-- the data with the same key into a single row.
combined_new_rows_without_fuel_cost AS (
  SELECT
    date,
    org_id,
    object_id,
    driver_id,
    interval_start,
    interval_end,
    SUM(COALESCE(fuel_consumed_ml, 0)) AS fuel_consumed_ml,
    SUM(COALESCE(energy_consumed_kwh, 0)) AS energy_consumed_kwh,
    SUM(COALESCE(distance_traveled_m, 0)) AS distance_traveled_m,
    0 AS custom_energy_cost,
    SUM(COALESCE(dynamic_energy_cost, 0)) AS dynamic_energy_cost,
    SUM(COALESCE(on_duration_ms, 0)) AS on_duration_ms,
    SUM(COALESCE(idle_duration_ms, 0)) AS idle_duration_ms,
    SUM(COALESCE(aux_during_idle_ms, 0)) AS aux_during_idle_ms,
    SUM(COALESCE(electric_distance_traveled_m, 0)) AS electric_distance_traveled_m,
    SUM(COALESCE(energy_regen_kwh, 0)) AS energy_regen_kwh
  FROM rows_without_fuel_cost
  WHERE date >= ${start_date}
  AND date < ${end_date}
  GROUP BY date, org_id, object_id, driver_id, interval_start, interval_end
),

combined_new_rows_with_fuel_cost AS (
SELECT
  cnr.*,
  COALESCE(cfci.custom_fuel_cost, dfci.dynamic_fuel_cost) * cnr.fuel_consumed_ml * uc.gallon_per_ml AS custom_fuel_cost,
  dfci.dynamic_fuel_cost * cnr.fuel_consumed_ml * uc.gallon_per_ml AS dynamic_fuel_cost
FROM
  combined_new_rows_without_fuel_cost cnr
  LEFT OUTER JOIN fuel_energy_efficiency_report.custom_fuel_unit_cost_interval AS cfci
  ON cnr.org_id = cfci.org_id
  AND cnr.interval_start = cfci.interval_start
  AND cnr.interval_end = cfci.interval_end
  LEFT OUTER JOIN fuel_energy_efficiency_report.dynamic_fuel_cost_hour_intervals AS dfci
  ON cnr.org_id = dfci.org_id
  AND cnr.interval_start = dfci.interval_start
  AND cnr.interval_end = dfci.interval_end
  CROSS JOIN helpers.unit_conversion uc
WHERE cnr.date >= ${start_date}
AND cnr.date < ${end_date}
),

canonical_fuel_types_with_interval_ends AS (
  SELECT
    DATE(created_at) as date,
    created_at,
    org_id,
    device_id,
    engine_type,
    gasoline_type,
    diesel_type,
    gaseous_type,
    hydrogen_type,
    configurable_fuel_id,
    DATE_TRUNC('hour', created_at) AS interval_start,
    COALESCE(
      LEAD(created_at) OVER (PARTITION BY org_id, device_id ORDER BY created_at ASC),
      CAST (${end_date} AS TIMESTAMP)
    ) AS interval_end
  FROM
   fueldb_shards.fuel_types
),

latest_canonical_fuel_type_start_ms_within_hour AS (
  SELECT
    MAX(created_at) AS oldest_start_ms,
    org_id,
    device_id,
    interval_start
  FROM
   canonical_fuel_types_with_interval_ends
  GROUP BY org_id, device_id, interval_start
),

effective_canonical_fuel_types_with_interval_ends AS (
  SELECT
    ft.date,
    ft.org_id,
    ft.device_id,
    ft.engine_type,
    ft.gasoline_type,
    ft.diesel_type,
    ft.gaseous_type,
    ft.hydrogen_type,
    ft.configurable_fuel_id,
    ft.interval_start,
    ft.interval_end
  FROM latest_canonical_fuel_type_start_ms_within_hour latest
  INNER JOIN canonical_fuel_types_with_interval_ends ft
  ON latest.org_id = ft.org_id
  AND latest.device_id = ft.device_id
  AND latest.oldest_start_ms = ft.created_at
),

-- Explode our intervals into hourly intervals.
canonical_fuel_type_hour_intervals AS (
  SELECT
    DATE(interval_start) AS date, -- Keep date in sync with the interval_start of the hourly interval
    org_id,
    interval_start,
    interval_start + INTERVAL 1 HOUR AS interval_end,
    device_id,
    engine_type,
    gasoline_type,
    diesel_type,
    gaseous_type,
    hydrogen_type,
    configurable_fuel_id
  FROM (
    SELECT
      date,
      org_id,
      device_id,
      engine_type,
      gasoline_type,
      diesel_type,
      gaseous_type,
      hydrogen_type,
      configurable_fuel_id,
      EXPLODE(
        SEQUENCE(
          DATE_TRUNC('hour', interval_start),
          DATE_TRUNC('hour', interval_end - INTERVAL 1 HOUR),
          INTERVAL 1 hour
        )
      ) AS interval_start
    FROM
      effective_canonical_fuel_types_with_interval_ends
    WHERE interval_start < interval_end
    AND interval_end >= ${start_date}
  )
  WHERE interval_start >= ${start_date}
    AND interval_start < ${end_date}
),


-- We now understand that electric_distance_traveled_m is only applicable for Hybrids.
-- For BEVs, electric_distance_traveled_m is inaccurate so we use distance_traveled_m instead.
combined_new_rows_with_corrected_ev_distance AS (
  SELECT
    cnr.date,
    cnr.org_id,
    cnr.object_id,
    cnr.driver_id,
    cnr.interval_start,
    cnr.interval_end,
    cnr.fuel_consumed_ml,
    cnr.energy_consumed_kwh,
    cnr.distance_traveled_m,
    cnr.custom_energy_cost,
    cnr.dynamic_energy_cost,
    cnr.on_duration_ms,
    cnr.idle_duration_ms,
    cnr.aux_during_idle_ms,
    IF (cft.engine_type = 1, cnr.distance_traveled_m, cnr.electric_distance_traveled_m) AS electric_distance_traveled_m, -- BEV is engine_type = 1
    cnr.energy_regen_kwh,
    cnr.custom_fuel_cost,
    cnr.dynamic_fuel_cost
  FROM
    combined_new_rows_with_fuel_cost cnr
    LEFT OUTER JOIN canonical_fuel_type_hour_intervals cft
    ON cnr.org_id = cft.org_id
    AND cnr.object_id = cft.device_id
    AND cnr.interval_start = cft.interval_start
    AND cnr.interval_end = cft.interval_end
)

SELECT
  *
FROM
  combined_new_rows_with_corrected_ev_distance
WHERE date >= ${start_date}
AND date < ${end_date}
