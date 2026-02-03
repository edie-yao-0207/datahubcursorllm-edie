-- This CTE is almost the same as in V3, we simply reference the new columns produced by V4 combined rows node
WITH aggregated_vehicle_driver_rows AS (
-- vehicle
SELECT
  date,
  cnr.org_id,
  cnr.object_id,
  1 AS object_type,
  cnr.interval_start,
  cnr.interval_end,
  MAX(cnr.engine_type) AS engine_type,
  MAX(cnr.configurable_fuel_id) AS configurable_fuel_id,
  COALESCE(SUM(cnr.fuel_consumed_ml), 0) AS fuel_consumed_ml,
  COALESCE(SUM(cnr.energy_consumed_kwh), 0) AS energy_consumed_kwh,
  COALESCE(SUM(cnr.gaseous_fuel_consumed_grams), 0) AS gaseous_fuel_consumed_grams,
  COALESCE(SUM(cnr.distance_traveled_m), 0) AS distance_traveled_m,
  COALESCE(SUM(cnr.fuel_cost), 0) AS fuel_cost,
  COALESCE(SUM(cnr.energy_cost), 0) AS energy_cost,
  COALESCE(SUM(cnr.gaseous_fuel_cost), 0) AS gaseous_fuel_cost,
  COALESCE(SUM(cnr.fuel_emissions), 0) AS fuel_emissions,
  COALESCE(SUM(cnr.energy_emissions), 0) AS energy_emissions,
  COALESCE(SUM(cnr.gaseous_fuel_emissions), 0) AS gaseous_fuel_emissions,
  COALESCE(SUM(cnr.on_duration_ms), 0) AS on_duration_ms,
  COALESCE(SUM(cnr.idle_duration_ms), 0) AS idle_duration_ms,
  SUM(cnr.aux_during_idle_ms) AS aux_during_idle_ms,
  COALESCE(SUM(cnr.electric_distance_traveled_m), 0) AS electric_distance_traveled_m,
  COALESCE(SUM(cnr.energy_regen_kwh), 0) AS energy_regen_kwh,
  COALESCE(SUM(cnr.fuel_consumed_ml_as_gaseous_type), 0) AS fuel_consumed_ml_as_gaseous_type,
  COALESCE(SUM(cnr.fuel_ml_cost_as_gaseous_type), 0) AS fuel_ml_cost_as_gaseous_type,
  COALESCE(SUM(cnr.fuel_ml_emissions_as_gaseous_type), 0) AS fuel_ml_emissions_as_gaseous_type
FROM
  fuel_energy_efficiency_report.combined_new_rows_v4 cnr
WHERE cnr.object_id IS NOT NULL
AND date >= ${start_date}
AND date < ${end_date}
GROUP BY org_id, object_id, interval_start, interval_end, date
UNION
-- driver
SELECT
  date,
  cnr.org_id,
  cnr.driver_id AS object_id,
  5 AS object_type,
  cnr.interval_start,
  cnr.interval_end,
  MAX(cnr.engine_type) AS engine_type,
  MAX(cnr.configurable_fuel_id) AS configurable_fuel_id,
  COALESCE(SUM(cnr.fuel_consumed_ml), 0) AS fuel_consumed_ml,
  COALESCE(SUM(cnr.energy_consumed_kwh), 0) AS energy_consumed_kwh,
  COALESCE(SUM(cnr.gaseous_fuel_consumed_grams), 0) AS gaseous_fuel_consumed_grams,
  COALESCE(SUM(cnr.distance_traveled_m), 0) AS distance_traveled_m,
  COALESCE(SUM(cnr.fuel_cost), 0) AS fuel_cost,
  COALESCE(SUM(cnr.energy_cost), 0) AS energy_cost,
  COALESCE(SUM(cnr.gaseous_fuel_cost), 0) AS gaseous_fuel_cost,
  COALESCE(SUM(cnr.fuel_emissions), 0) AS fuel_emissions,
  COALESCE(SUM(cnr.energy_emissions), 0) AS energy_emissions,
  COALESCE(SUM(cnr.gaseous_fuel_emissions), 0) AS gaseous_fuel_emissions,
  COALESCE(SUM(cnr.on_duration_ms), 0) AS on_duration_ms,
  COALESCE(SUM(cnr.idle_duration_ms), 0) AS idle_duration_ms,
  SUM(cnr.aux_during_idle_ms) AS aux_during_idle_ms,
  COALESCE(SUM(cnr.electric_distance_traveled_m), 0) AS electric_distance_traveled_m,
  COALESCE(SUM(cnr.energy_regen_kwh), 0) AS energy_regen_kwh,
  COALESCE(SUM(cnr.fuel_consumed_ml_as_gaseous_type), 0) AS fuel_consumed_ml_as_gaseous_type,
  COALESCE(SUM(cnr.fuel_ml_cost_as_gaseous_type), 0) AS fuel_ml_cost_as_gaseous_type,
  COALESCE(SUM(cnr.fuel_ml_emissions_as_gaseous_type), 0) AS fuel_ml_emissions_as_gaseous_type
FROM
  fuel_energy_efficiency_report.combined_new_rows_v4 cnr
WHERE cnr.driver_id != 0
AND date >= ${start_date}
AND date < ${end_date}
GROUP BY org_id, driver_id, interval_start, interval_end, date
),

-- 1 meter = 0.0006213711922 miles
-- 1 MPGEe = 33,705 watt-hours per mile
-- 1 ml = 0.000264172 gallons
-- This CTE is the same as in V3
with_mpg AS (
SELECT
  *,
  CASE -- we need mpg calculations to know which hourly data points to throw out
    WHEN (fuel_consumed_ml + fuel_consumed_ml_as_gaseous_type) > 0 and energy_consumed_kwh > 0
      THEN electric_distance_traveled_m / distance_traveled_m * 33705 / (energy_consumed_kwh * 1000 / (electric_distance_traveled_m * 0.0006213711922)) +
          (1 - electric_distance_traveled_m / distance_traveled_m) * (distance_traveled_m - electric_distance_traveled_m) * 0.0006213711922 / ((fuel_consumed_ml + fuel_consumed_ml_as_gaseous_type) * 0.000264172)
    WHEN (fuel_consumed_ml + fuel_consumed_ml_as_gaseous_type) = 0 AND energy_consumed_kwh > 0
      THEN 33705 / (energy_consumed_kwh * 1000 / (electric_distance_traveled_m * 0.0006213711922))
    WHEN (fuel_consumed_ml + fuel_consumed_ml_as_gaseous_type) > 0 AND energy_consumed_kwh = 0
      THEN distance_traveled_m * 0.0006213711922 / ((fuel_consumed_ml + fuel_consumed_ml_as_gaseous_type) * 0.000264172)
    ELSE 0
  END weighted_mpge,
  CASE
    -- 1kg of gaseous fuel = 50MJ. 1liter of diesel = 35.8MJ. Therefore 1kg of gaseous fuel = 1400 ml of diesel.
    -- We use this to calculate the MPGe for gaseous fuel.
    WHEN gaseous_fuel_consumed_grams > 0
      THEN (distance_traveled_m * 0.0006213711922) / (gaseous_fuel_consumed_grams * 1.4 * 0.000264172)
    ELSE 0
  END gaseous_mpg,
  CASE
    WHEN gaseous_fuel_consumed_grams > 0 THEN true
    ELSE false
  END has_used_gaseous_fuel,
  CASE -- record energy consumed value to know which threshold to use
    WHEN energy_consumed_kwh > 0 THEN 'Y'
    ELSE 'N'
  END has_used_energy,
  CASE
    WHEN (fuel_consumed_ml + fuel_consumed_ml_as_gaseous_type) = 0 AND energy_consumed_kwh = 0 THEN 'N'
    ELSE 'Y'
  END has_used_fuel_or_energy
FROM aggregated_vehicle_driver_rows
WHERE date >= ${start_date}
AND date < ${end_date}
),

-- This CTE is almost the same as in V3, we apply the existing logic to the new columns fuel cost, fuel emissions, energy cost and energy emissions.
-- The purpose of this CTE is to filter out values when we detect that the efficiency is absurdly high.
-- >200MPGe for vehicles that have used energy (Hybrids, BEVs, PHEVs)
-- >65MPG for vehicles that have just used fuel (ICEs)
with_corrected AS (
SELECT
    date,
    org_id,
    object_id,
    object_type,
    interval_start,
    interval_end,
    engine_type,
    configurable_fuel_id,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE fuel_consumed_ml
    END fuel_consumed_ml,
    fuel_consumed_ml AS fuel_consumed_ml_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE fuel_consumed_ml_as_gaseous_type
    END fuel_consumed_ml_as_gaseous_type,
    fuel_consumed_ml_as_gaseous_type AS fuel_consumed_ml_as_gaseous_type_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE energy_consumed_kwh
    END energy_consumed_kwh,
    energy_consumed_kwh AS energy_consumed_kwh_original,
    CASE
      WHEN has_used_gaseous_fuel = 'N' THEN 0
      WHEN has_used_gaseous_fuel = 'Y' AND gaseous_mpg > 65 THEN 0
      ELSE gaseous_fuel_consumed_grams
    END gaseous_fuel_consumed_grams,
    gaseous_fuel_consumed_grams AS gaseous_fuel_consumed_grams_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND has_used_gaseous_fuel = 'N' AND weighted_mpge > 65 THEN 0
      WHEN has_used_energy = 'N' AND has_used_gaseous_fuel = 'Y' AND gaseous_mpg > 65 THEN 0
      ELSE distance_traveled_m
    END distance_traveled_m,
    distance_traveled_m AS distance_traveled_m_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE fuel_cost
    END fuel_cost,
    fuel_cost as fuel_cost_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE fuel_ml_cost_as_gaseous_type
    END fuel_ml_cost_as_gaseous_type,
    fuel_ml_cost_as_gaseous_type AS fuel_ml_cost_as_gaseous_type_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE energy_cost
    END energy_cost,
    energy_cost as energy_cost_original,
    CASE
      WHEN has_used_gaseous_fuel = 'N' THEN 0
      WHEN has_used_gaseous_fuel = 'Y' AND gaseous_mpg > 65 THEN 0
      ELSE gaseous_fuel_cost
    END gaseous_fuel_cost,
    gaseous_fuel_cost AS gaseous_fuel_cost_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE fuel_emissions
    END fuel_emissions,
    fuel_emissions as fuel_emissions_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE fuel_ml_emissions_as_gaseous_type
    END fuel_ml_emissions_as_gaseous_type,
    fuel_ml_emissions_as_gaseous_type AS fuel_ml_emissions_as_gaseous_type_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE energy_emissions
    END energy_emissions,
    energy_emissions as energy_emissions_original,
    CASE
      WHEN has_used_gaseous_fuel = 'N' THEN 0
      WHEN has_used_gaseous_fuel = 'Y' AND gaseous_mpg > 65 THEN 0
      ELSE gaseous_fuel_emissions
    END gaseous_fuel_emissions,
    gaseous_fuel_emissions AS gaseous_fuel_emissions_original,
    on_duration_ms,
    idle_duration_ms,
    aux_during_idle_ms,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      -- weighted_mpge can be null if canonical distance is 0 because of division by zero.
      -- This can happen if the vehicle didn't actually travel but there's non-zero
      -- electric odometer data.
      WHEN has_used_energy = 'Y' AND (weighted_mpge IS NULL OR weighted_mpge > 200) THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE electric_distance_traveled_m
    END electric_distance_traveled_m,
    electric_distance_traveled_m AS electric_distance_traveled_m_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND (weighted_mpge IS NULL OR weighted_mpge > 200) THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE energy_regen_kwh
    END energy_regen_kwh,
    energy_regen_kwh AS energy_regen_kwh_original
FROM with_mpg
WHERE date >= ${start_date}
AND date < ${end_date}
),

-- This CTE is new for V4
latest_canonical_fuel_type_times_as_of_cutover AS (
  SELECT
    org_id,
    device_id,
    MAX(created_at) AS max_created_at
  FROM
    fueldb_shards.fuel_types
  WHERE created_at < '2024-04-01' -- Take a snapshot of canonical fuel types prior to this date, while we copy data from V3 data prior to this date.
  GROUP BY
    org_id,
    device_id
),

fuel_type_info_for_v3_backfill AS (
  SELECT
  ft.org_id AS org_id,
  ft.device_id AS device_id,
  ft.engine_type AS engine_type,
  ft.configurable_fuel_id AS configurable_fuel_id,
  CASE ft.configurable_fuel_id
    WHEN 0 THEN 2329 -- Gasoline
    WHEN 1 THEN 2614 -- Diesel
    WHEN 2 THEN 2485 -- Biodiesel
    WHEN 3 THEN 1504 -- Ethanol
    ELSE 2329 -- Default to Gasoline
  END AS emissions_grams_per_liter
  FROM fueldb_shards.fuel_types AS ft
  INNER JOIN latest_canonical_fuel_type_times_as_of_cutover AS latest_ft
    ON ft.org_id = latest_ft.org_id
    AND ft.device_id = latest_ft.device_id
    AND ft.created_at = latest_ft.max_created_at
),

-- This CTE is new for V4
historical_v3_data_with_emissions AS (
  SELECT
    date,
    v3.org_id,
    object_id,
    object_type,
    IF(object_type = 1, COALESCE(historical_fuel_type_info.engine_type, 0), 0) AS engine_type, -- Default to ICE if driver row, we have no way to find the right engine type. Or if the canonical fuel type record doesn't exist.
    COALESCE(historical_fuel_type_info.configurable_fuel_id,  0) AS configurable_fuel_id, -- Default to ICE if driver row or canonical fuel type record doesn't exist.
    interval_start,
    interval_start + INTERVAL 1 HOUR AS interval_end,
    fuel_consumed_ml,
    fuel_consumed_ml_original,
    energy_consumed_kwh,
    energy_consumed_kwh_original,
    distance_traveled_m,
    distance_traveled_m_original,
    distance_traveled_m_gps,
    distance_traveled_m_gps_original,
    distance_traveled_m_odo,
    distance_traveled_m_odo_original,
    COALESCE(IF(custom_fuel_cost != 0 AND custom_fuel_cost IS NOT NULL, custom_fuel_cost, dynamic_fuel_cost), 0) AS fuel_cost,
    COALESCE(IF(custom_energy_cost != 0 AND custom_energy_cost IS NOT NULL, custom_energy_cost, dynamic_energy_cost), 0) AS energy_cost,
    COALESCE(historical_fuel_type_info.emissions_grams_per_liter * fuel_consumed_ml * 0.001, 2329 * fuel_consumed_ml/1000) AS fuel_emissions, -- Defaults to using gasoline emissions factor (eg. for driver rows/when canonical fuel type record doesn't exist).
    0 AS energy_emissions, -- Energy Emissions are only possible with a custom override, which will not be possible at the time of cutover.
    on_duration_ms,
    idle_duration_ms,
    aux_during_idle_ms,
    electric_distance_traveled_m,
    electric_distance_traveled_m_original,
    energy_regen_kwh,
    energy_regen_kwh_original
  FROM fuel_maintenance.historical_fuel_energy_report_delta_v3 v3
  LEFT JOIN fuel_type_info_for_v3_backfill AS historical_fuel_type_info
    ON v3.org_id = historical_fuel_type_info.org_id
    AND v3.object_id = historical_fuel_type_info.device_id
  WHERE date < "2024-04-01" -- Use/copy from V3 data prior to this date. This is prior to when canonical fuel types was fully ready.
    AND date >= ${start_date}
    AND date < ${end_date}
),

-- To keep historical FEER data the same we now combine FEER v3 and v4 data
-- This CTE is new for V4
unioned AS (
-- Select rows from v3
SELECT
  date,
  org_id,
  object_id,
  object_type,
  engine_type,
  configurable_fuel_id,
  interval_start,
  interval_end,
  fuel_consumed_ml,
  fuel_consumed_ml_original,
  energy_consumed_kwh,
  energy_consumed_kwh_original,
  distance_traveled_m,
  distance_traveled_m_original,
  distance_traveled_m_gps,
  distance_traveled_m_gps_original,
  distance_traveled_m_odo,
  distance_traveled_m_odo_original,
  fuel_cost,
  energy_cost,
  fuel_emissions,
  energy_emissions,
  fuel_cost AS fuel_cost_original,
  energy_cost AS energy_cost_original,
  fuel_emissions AS fuel_emissions_original,
  energy_emissions AS energy_emissions_original,
  on_duration_ms,
  idle_duration_ms,
  aux_during_idle_ms,
  electric_distance_traveled_m,
  electric_distance_traveled_m_original,
  energy_regen_kwh,
  energy_regen_kwh_original
FROM historical_v3_data_with_emissions
WHERE date < "2024-04-01" -- Copy from V3 data prior to this date. This is prior to when canonical fuel types was fully ready.
  AND date >= ${start_date}
  AND date < ${end_date}
UNION ALL -- Must use UNION ALL otherwise we encounter a "GC overhead limit exceeded" error when running tests, rows will be distinct anyway due to date filtering
-- Select rows from v4
SELECT
  date,
  org_id,
  object_id,
  object_type,
  engine_type,
  configurable_fuel_id,
  interval_start,
  interval_end,
  fuel_consumed_ml,
  fuel_consumed_ml_original,
  energy_consumed_kwh,
  energy_consumed_kwh_original,
  distance_traveled_m,
  distance_traveled_m_original,
  NULL AS distance_traveled_m_gps,
  NULL AS distance_traveled_m_gps_original,
  NULL AS distance_traveled_m_odo,
  NULL AS distance_traveled_m_odo_original,
  fuel_cost,
  energy_cost,
  fuel_emissions,
  energy_emissions,
  fuel_cost_original,
  energy_cost_original,
  fuel_emissions_original,
  energy_emissions_original,
  on_duration_ms,
  idle_duration_ms,
  aux_during_idle_ms,
  electric_distance_traveled_m,
  electric_distance_traveled_m_original,
  energy_regen_kwh,
  energy_regen_kwh_original
FROM with_corrected
WHERE date >= "2024-04-01" -- Start using V4 logic/data from this date. This is when canonical fuel types was fully ready.
  AND date >= ${start_date}
  AND date < ${end_date}
  AND (fuel_consumed_ml + fuel_consumed_ml_original + energy_consumed_kwh + energy_consumed_kwh_original
    + COALESCE(distance_traveled_m, 0) + COALESCE(distance_traveled_m_original, 0)
    + fuel_cost + energy_cost + fuel_emissions + energy_emissions
    + fuel_cost_original + energy_cost_original + fuel_emissions_original + energy_emissions_original
    + on_duration_ms + idle_duration_ms
    + electric_distance_traveled_m + electric_distance_traveled_m_original + energy_regen_kwh + energy_regen_kwh_original
    + COALESCE(aux_during_idle_ms, 0)
    ) != 0
),

-- We need to rejoin the new gaseous fuel columns as otherwise the tests hang (potentially due to the huge UNION)
unioned_with_gaseous_fuel AS (
  SELECT
    unioned.*,
    with_corrected.gaseous_fuel_consumed_grams AS gaseous_fuel_consumed_grams,
    with_corrected.gaseous_fuel_consumed_grams_original AS gaseous_fuel_consumed_grams_original,
    with_corrected.gaseous_fuel_cost AS gaseous_fuel_cost,
    with_corrected.gaseous_fuel_cost_original AS gaseous_fuel_cost_original,
    with_corrected.gaseous_fuel_emissions AS gaseous_fuel_emissions,
    with_corrected.gaseous_fuel_emissions_original AS gaseous_fuel_emissions_original,

    with_corrected.fuel_consumed_ml_as_gaseous_type AS fuel_consumed_ml_as_gaseous_type,
    with_corrected.fuel_consumed_ml_as_gaseous_type_original AS fuel_consumed_ml_as_gaseous_type_original,
    with_corrected.fuel_ml_cost_as_gaseous_type AS fuel_ml_cost_as_gaseous_type,
    with_corrected.fuel_ml_cost_as_gaseous_type_original AS fuel_ml_cost_as_gaseous_type_original,
    with_corrected.fuel_ml_emissions_as_gaseous_type AS fuel_ml_emissions_as_gaseous_type,
    with_corrected.fuel_ml_emissions_as_gaseous_type_original AS fuel_ml_emissions_as_gaseous_type_original
  FROM unioned
  LEFT JOIN with_corrected
    ON unioned.date = with_corrected.date
    AND unioned.org_id = with_corrected.org_id
    AND unioned.object_id = with_corrected.object_id
    AND unioned.object_type = with_corrected.object_type
    AND unioned.interval_start = with_corrected.interval_start

)

SELECT
  date,
  org_id,
  object_id,
  object_type,
  engine_type,
  configurable_fuel_id,
  interval_start,
  fuel_consumed_ml,
  fuel_consumed_ml_original,
  fuel_consumed_ml_as_gaseous_type,
  fuel_consumed_ml_as_gaseous_type_original,
  energy_consumed_kwh,
  energy_consumed_kwh_original,
  gaseous_fuel_consumed_grams,
  gaseous_fuel_consumed_grams_original,
  distance_traveled_m,
  distance_traveled_m_original,
  distance_traveled_m_gps,
  distance_traveled_m_gps_original,
  distance_traveled_m_odo,
  distance_traveled_m_odo_original,
  fuel_cost,
  fuel_ml_cost_as_gaseous_type,
  energy_cost,
  gaseous_fuel_cost,
  fuel_emissions,
  fuel_ml_emissions_as_gaseous_type,
  energy_emissions,
  gaseous_fuel_emissions,
  fuel_cost_original,
  fuel_ml_cost_as_gaseous_type_original,
  energy_cost_original,
  gaseous_fuel_cost_original,
  fuel_emissions_original,
  fuel_ml_emissions_as_gaseous_type_original,
  energy_emissions_original,
  gaseous_fuel_emissions_original,
  on_duration_ms,
  idle_duration_ms,
  aux_during_idle_ms,
  electric_distance_traveled_m,
  electric_distance_traveled_m_original,
  energy_regen_kwh,
  energy_regen_kwh_original
FROM unioned_with_gaseous_fuel
