-- This CTE is almost the same as in V3
-- We remove the calculation of the energy cost as it is replaced by the new custom fuel cost settings system which is computed in the penultimate CTE in this file
WITH rows_without_fuel_cost AS (
  SELECT
    COALESCE(esh.date, eh.date, evd.date, rh.date, dh.date) AS date,
    COALESCE(esh.org_id, eh.org_id, evd.org_id, rh.org_id, dh.org_id) AS org_id,
    COALESCE(esh.object_id, eh.object_id, evd.object_id, rh.object_id, dh.object_id) AS object_id,
    COALESCE(esh.driver_id, eh.driver_id, evd.driver_id, rh.driver_id, dh.driver_id, 0) AS driver_id,
    COALESCE(esh.interval_start, eh.interval_start, evd.interval_start, rh.interval_start, dh.interval_start) AS interval_start,
    COALESCE(esh.interval_start, eh.interval_start, evd.interval_start, rh.interval_start, dh.interval_start) + INTERVAL 1 HOURS as interval_end,
    esh.fuel_consumed_ml AS fuel_consumed_ml,
    esh.gaseous_fuel_consumed_grams AS gaseous_fuel_consumed_grams,
    eh.energy_consumed_kwh AS energy_consumed_kwh,
    dh.distance_traveled_m AS distance_traveled_m,
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
-- This CTE is the same as in V3
combined_new_rows_without_fuel_cost AS (
  SELECT
    date,
    org_id,
    object_id,
    driver_id,
    interval_start,
    interval_end,
    SUM(COALESCE(fuel_consumed_ml, 0)) AS fuel_consumed_ml,
    SUM(COALESCE(gaseous_fuel_consumed_grams, 0)) AS gaseous_fuel_consumed_grams,
    SUM(COALESCE(energy_consumed_kwh, 0)) AS energy_consumed_kwh,
    SUM(COALESCE(distance_traveled_m, 0)) AS distance_traveled_m,
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

-- Calculate canonical fuel type intervals
canonical_fuel_types_with_interval_ends AS (
  SELECT
    created_at AS interval_start,
    org_id,
    device_id,
    engine_type,
    configurable_fuel_id,
    COALESCE(
      LEAD(created_at) OVER (PARTITION BY (org_id, device_id) ORDER BY created_at ASC),
      CAST (${end_date} AS TIMESTAMP)
    ) AS interval_end
  FROM
   fueldb_shards.fuel_types
),

latest_canonical_fuel_type_start_ms_within_hour AS (
  SELECT
    org_id,
    device_id,
    MAX(interval_start) AS oldest_start_ms
  FROM
   canonical_fuel_types_with_interval_ends
  GROUP BY org_id, device_id, DATE_TRUNC('hour', interval_start)
),

effective_canonical_fuel_types_with_interval_ends AS (
  SELECT
    ft.org_id,
    ft.device_id,
    ft.engine_type,
    ft.configurable_fuel_id,
    IF(ft.interval_start < (CAST (${start_date} AS TIMESTAMP) - INTERVAL 1 HOUR), (CAST (${start_date} AS TIMESTAMP) - INTERVAL 1 HOUR), ft.interval_start) AS interval_start,
    ft.interval_end
  FROM latest_canonical_fuel_type_start_ms_within_hour latest
  INNER JOIN canonical_fuel_types_with_interval_ends ft
  ON latest.org_id = ft.org_id
  AND latest.device_id = ft.device_id
  AND latest.oldest_start_ms = ft.interval_start
),

-- Explode our intervals into hourly intervals.
canonical_fuel_type_hour_intervals AS (
  SELECT
    DATE(interval_start) AS date,
    org_id,
    interval_start,
    interval_start + INTERVAL 1 HOUR AS interval_end,
    device_id,
    engine_type,
    configurable_fuel_id
  FROM (
    SELECT
      org_id,
      device_id,
      engine_type,
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
    AND interval_end > ${start_date}
  )
),

-- Filter for Gasoline cost/emissions hour intervals to use as the default for conventional fuel consumed (measured in us gal).
gasoline_fuel_cost_hour_intervals AS (
  SELECT
    date,
    org_id,
    cost_per_us_gal,
    emissions_grams_per_us_gal,
    interval_start,
    interval_end
  FROM fuel_energy_efficiency_report.org_fuel_energy_cost_and_emissions_hour_intervals
  WHERE configurable_fuel_id = 0 -- Gasoline Id
),

-- Filter for Electricity cost hour intervals to use as the energy factor for PHEV and BEV vehicles
energy_fuel_cost_hour_intervals AS (
  SELECT
    date,
    org_id,
    cost_per_kwh,
    emissions_grams_per_wh,
    interval_start,
    interval_end
  FROM fuel_energy_efficiency_report.org_fuel_energy_cost_and_emissions_hour_intervals
  WHERE configurable_fuel_id = 4 -- Electricity Id
),


rows_with_fuel_type AS (
  SELECT
    cnr.date,
    cnr.org_id,
    cnr.object_id,
    cnr.driver_id,
    cnr.interval_start,
    cnr.interval_end,
    cnr.fuel_consumed_ml,
    cnr.gaseous_fuel_consumed_grams,
    cnr.energy_consumed_kwh,
    cnr.distance_traveled_m,
    cnr.on_duration_ms,
    cnr.idle_duration_ms,
    cnr.aux_during_idle_ms,
    cnr.electric_distance_traveled_m,
    cnr.energy_regen_kwh,

    -- Default to unknown engine_type (6) and fuel_id (99)
    COALESCE(fuel_type_intervals.engine_type, 6) AS engine_type,
    COALESCE(fuel_type_intervals.configurable_fuel_id, 99) AS configurable_fuel_id
  FROM combined_new_rows_without_fuel_cost AS cnr
  LEFT JOIN canonical_fuel_type_hour_intervals AS fuel_type_intervals
    ON fuel_type_intervals.org_id = cnr.org_id
    AND fuel_type_intervals.device_id = cnr.object_id
    AND fuel_type_intervals.interval_start = cnr.interval_start
    AND fuel_type_intervals.interval_end = cnr.interval_end
),

rows_with_is_applicable_gaseous_fuel_type AS (
  SELECT
    *,
    IF(configurable_fuel_id IN (8, 9, 11), true, false) AS is_applicable_gaseous_fuel_type, -- 8: CNG, 9: LNG, 11: RNG
    -- Include a cutover date from when we should move the values over.
    -- This is so we have time to merge the read path changes before the data pipeline changes take effect as otherwise there would be data change for customers.
    IF(date >= "2024-09-26", true, false) AS after_move_gaseous_ml_cutover_date
  FROM rows_with_fuel_type
),

rows_with_fuel_consumed_ml_as_gaseous_vehicle AS (
  SELECT
    date,
    org_id,
    object_id,
    driver_id,
    interval_start,
    interval_end,
    engine_type,
    configurable_fuel_id,
    -- For gaseous vehicles, we want to eliminate double counted fuel consumption values (fuel consumption reported twice for the same work done).
    -- We can do this and allow the correct source to be chosen in the service by moving the fuel_consumed_ml to a new column for gaseous vehicles.
    -- This allows us to know the fuel_consumed_ml value for the gaseous vehicle segment even after the rollup step.
    IF(is_applicable_gaseous_fuel_type and after_move_gaseous_ml_cutover_date, 0, fuel_consumed_ml) AS fuel_consumed_ml,
    IF(is_applicable_gaseous_fuel_type and after_move_gaseous_ml_cutover_date, fuel_consumed_ml, 0) AS fuel_consumed_ml_as_gaseous_type,
    IF(is_applicable_gaseous_fuel_type, gaseous_fuel_consumed_grams, 0) AS gaseous_fuel_consumed_grams,
    energy_consumed_kwh,
    distance_traveled_m,
    on_duration_ms,
    idle_duration_ms,
    aux_during_idle_ms,
    electric_distance_traveled_m,
    energy_regen_kwh,
    engine_type,
    configurable_fuel_id
  FROM rows_with_is_applicable_gaseous_fuel_type
),

-- Energy consumed as reported by the vehicle does not take into account energy regen.
-- We need to subtract energy regen from energy consumed to get the actual energy consumed so that is accurate for our reporting and cost/emissions calculations.
rows_with_energy_regen_subtracted_from_energy_consumed AS (
  SELECT
    date,
    org_id,
    object_id,
    driver_id,
    interval_start,
    interval_end,
    engine_type,
    configurable_fuel_id,
    fuel_consumed_ml,
    fuel_consumed_ml_as_gaseous_type,
    gaseous_fuel_consumed_grams,
    distance_traveled_m,
    on_duration_ms,
    idle_duration_ms,
    aux_during_idle_ms,
    electric_distance_traveled_m,
    energy_regen_kwh,
    IF(engine_type = 4 AND configurable_fuel_id = 12, energy_consumed_kwh, energy_consumed_kwh - energy_regen_kwh) AS energy_consumed_kwh -- Hydrogen-FCEV vehicles have an issue whereby energy regen > energy consumed. As a temporary fix we therefore ignore energy regen for Hydrogen-FCEV vehicles.
  FROM rows_with_fuel_consumed_ml_as_gaseous_vehicle
),

-- Finally we combine our vehicle fuel id hourly intervals with the fuel cost/emissions hourly intervals
-- Falling back to default values where needed
combined_new_rows_with_fuel_energy_cost_emissions AS (
  SELECT
    cnr.date,
    cnr.org_id,
    cnr.object_id,
    cnr.driver_id,
    cnr.interval_start,
    cnr.interval_end,
    cnr.fuel_consumed_ml,
    cnr.fuel_consumed_ml_as_gaseous_type,
    cnr.gaseous_fuel_consumed_grams,
    cnr.energy_consumed_kwh,
    cnr.distance_traveled_m,
    cnr.on_duration_ms,
    cnr.idle_duration_ms,
    cnr.aux_during_idle_ms,
    cnr.electric_distance_traveled_m,
    cnr.energy_regen_kwh,
    cnr.engine_type,
    cnr.configurable_fuel_id,

    -- For Energy Cost, Per unit is equal to per Kilowatt Hour
    -- Priority: Configurable Fuel Id factor, Fixed Cost per Wh
    COALESCE(energy_factor_intervals.cost_per_kwh, uc.cost_per_kwh) * cnr.energy_consumed_kwh AS energy_cost,

    -- For Energy Emissions, Per unit is equal to per Watt Hour
    COALESCE(energy_factor_intervals.emissions_grams_per_wh, 0) * 1000 * cnr.energy_consumed_kwh AS energy_emissions,

    -- For Fuel Cost, Per unit is equal to per US Gallon
    -- Priority: Specific Configurable Fuel Id factor, Gasoline Configurable Fuel Id Factor
    COALESCE(fuel_type_factor_intervals.cost_per_us_gal, gasoline_fuel_cost_hour_intervals.cost_per_us_gal) * cnr.fuel_consumed_ml * uc.gallon_per_ml  AS fuel_cost,
    COALESCE(fuel_type_factor_intervals.cost_per_us_gal, gasoline_fuel_cost_hour_intervals.cost_per_us_gal) * cnr.fuel_consumed_ml_as_gaseous_type * uc.gallon_per_ml  AS fuel_ml_cost_as_gaseous_type,


    -- For Fuel Emissions, Per unit is equal to per US Gallon
    -- Priority: Specific Configurable Fuel Id factor, Default Fuel Factor (Gasoline, Otherwise a fixed emission factor)
    COALESCE(fuel_type_factor_intervals.emissions_grams_per_us_gal, gasoline_fuel_cost_hour_intervals.emissions_grams_per_us_gal)  * cnr.fuel_consumed_ml * uc.gallon_per_ml AS fuel_emissions,
    COALESCE(fuel_type_factor_intervals.emissions_grams_per_us_gal, gasoline_fuel_cost_hour_intervals.emissions_grams_per_us_gal)  * cnr.fuel_consumed_ml_as_gaseous_type * uc.gallon_per_ml AS fuel_ml_emissions_as_gaseous_type,

    -- For Gaseous Cost, Per unit is equal to per kg
    -- Priority: Specific Configurable Fuel Id factor, Default of 0 as this has been defined for all relevant gaseous types.
    COALESCE(fuel_type_factor_intervals.cost_per_kg, 0) * (cnr.gaseous_fuel_consumed_grams / 1000) AS gaseous_fuel_cost,

    -- For Gaseous Emissions, Per unit is equal to per kg
    -- Priority: Specific Configurable Fuel Id factor, Default of 0 as the specific factor should always be available as gathered weekly via our fuel cost API integration for all relevant gaseous types.
    COALESCE(fuel_type_factor_intervals.emissions_grams_per_kg, 0) * (cnr.gaseous_fuel_consumed_grams / 1000) AS gaseous_fuel_emissions

  FROM rows_with_energy_regen_subtracted_from_energy_consumed AS cnr
  LEFT JOIN fuel_energy_efficiency_report.org_fuel_energy_cost_and_emissions_hour_intervals AS fuel_type_factor_intervals
    ON fuel_type_factor_intervals.configurable_fuel_id = cnr.configurable_fuel_id AND fuel_type_factor_intervals.configurable_fuel_id != 4 -- Electricity Id, we already apply this as the energy factor.
    AND fuel_type_factor_intervals.org_id = cnr.org_id
    AND fuel_type_factor_intervals.interval_start = cnr.interval_start
    AND fuel_type_factor_intervals.interval_end = cnr.interval_end
  LEFT JOIN gasoline_fuel_cost_hour_intervals AS gasoline_fuel_cost_hour_intervals
    ON gasoline_fuel_cost_hour_intervals.org_id = cnr.org_id
    AND gasoline_fuel_cost_hour_intervals.interval_start = cnr.interval_start
    AND gasoline_fuel_cost_hour_intervals.interval_end = cnr.interval_end
  LEFT JOIN energy_fuel_cost_hour_intervals AS energy_factor_intervals
    ON energy_factor_intervals.org_id = cnr.org_id
    AND energy_factor_intervals.interval_start = cnr.interval_start
    AND energy_factor_intervals.interval_end = cnr.interval_end
  CROSS JOIN helpers.unit_conversion uc
),

-- We now understand that electric_distance_traveled_m is only applicable for Hybrids.
-- For BEVs, electric_distance_traveled_m is inaccurate so we use distance_traveled_m instead.
-- Additionally vehicles that are Hydrogen-FCEV should be treated like BEVs as they report electric consumption stats.
combined_new_rows_with_corrected_ev_distance AS (
  SELECT
    date,
    org_id,
    object_id,
    driver_id,
    interval_start,
    interval_end,
    engine_type,
    configurable_fuel_id,
    fuel_consumed_ml,
    fuel_consumed_ml_as_gaseous_type,
    gaseous_fuel_consumed_grams,
    energy_consumed_kwh,
    distance_traveled_m,
    on_duration_ms,
    idle_duration_ms,
    aux_during_idle_ms,
    IF(engine_type = 1 OR (engine_type = 4 AND configurable_fuel_id = 12), distance_traveled_m, electric_distance_traveled_m) AS electric_distance_traveled_m, -- BEV is engine_type = 1, Hydrogen-FCEV is engine_type = 4 and configurable_fuel_id = 12
    energy_regen_kwh,
    fuel_cost,
    fuel_ml_cost_as_gaseous_type,
    energy_cost,
    gaseous_fuel_cost,
    fuel_emissions,
    fuel_ml_emissions_as_gaseous_type,
    energy_emissions,
    gaseous_fuel_emissions
  FROM combined_new_rows_with_fuel_energy_cost_emissions
)



SELECT
    date,
    org_id,
    object_id,
    driver_id,
    interval_start,
    interval_end,
    engine_type,
    configurable_fuel_id,
    fuel_consumed_ml,
    fuel_consumed_ml_as_gaseous_type,
    gaseous_fuel_consumed_grams,
    CAST(energy_consumed_kwh AS DECIMAL(38, 9)) AS energy_consumed_kwh, -- Ensure that the column is consistent with the current schema.
    distance_traveled_m,
    on_duration_ms,
    idle_duration_ms,
    aux_during_idle_ms,
    electric_distance_traveled_m,
    energy_regen_kwh,
    fuel_cost,
    fuel_ml_cost_as_gaseous_type,
    energy_cost,
    gaseous_fuel_cost,
    fuel_emissions,
    fuel_ml_emissions_as_gaseous_type,
    energy_emissions,
    gaseous_fuel_emissions
FROM combined_new_rows_with_corrected_ev_distance
  WHERE date >= ${start_date}
  AND date < ${end_date}
