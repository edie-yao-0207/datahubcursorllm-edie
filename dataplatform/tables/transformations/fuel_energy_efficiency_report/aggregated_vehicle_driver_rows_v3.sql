-- To have maximum data when testing we will not yet bring in FEER v2/historical data to this pipeline.
-- However when testing is completed, we will need to modify the logic here to bring in FEER v2 data
-- for orgs that were switched over to FEER v2 and potentially FEER v1 data if any orgs skipped from v1 to v3

WITH aggregated_vehicle_driver_rows AS (
-- vehicle
SELECT
  date,
  cnr.org_id,
  cnr.object_id,
  1 AS object_type,
  cnr.interval_start,
  cnr.interval_end,
  COALESCE(SUM(cnr.fuel_consumed_ml), 0) AS fuel_consumed_ml,
  COALESCE(SUM(cnr.energy_consumed_kwh), 0) AS energy_consumed_kwh,
  COALESCE(SUM(cnr.distance_traveled_m), 0) AS distance_traveled_m,
  COALESCE(SUM(cnr.custom_fuel_cost), 0) AS custom_fuel_cost,
  COALESCE(SUM(cnr.dynamic_fuel_cost), 0) AS dynamic_fuel_cost,
  COALESCE(SUM(cnr.custom_energy_cost), 0) AS custom_energy_cost,
  COALESCE(SUM(cnr.dynamic_energy_cost), 0) AS dynamic_energy_cost,
  COALESCE(SUM(cnr.on_duration_ms), 0) AS on_duration_ms,
  COALESCE(SUM(cnr.idle_duration_ms), 0) AS idle_duration_ms,
  SUM(cnr.aux_during_idle_ms) AS aux_during_idle_ms,
  COALESCE(SUM(cnr.electric_distance_traveled_m), 0) AS electric_distance_traveled_m,
  COALESCE(SUM(cnr.energy_regen_kwh), 0) AS energy_regen_kwh
FROM
  fuel_energy_efficiency_report.combined_new_rows_v3 cnr
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
  COALESCE(SUM(cnr.fuel_consumed_ml), 0) AS fuel_consumed_ml,
  COALESCE(SUM(cnr.energy_consumed_kwh), 0) AS energy_consumed_kwh,
  COALESCE(SUM(cnr.distance_traveled_m), 0) AS distance_traveled_m,
  COALESCE(SUM(cnr.custom_fuel_cost), 0) AS custom_fuel_cost,
  COALESCE(SUM(cnr.dynamic_fuel_cost), 0) AS dynamic_fuel_cost,
  COALESCE(SUM(cnr.custom_energy_cost), 0) AS custom_energy_cost,
  COALESCE(SUM(cnr.dynamic_energy_cost), 0) AS dynamic_energy_cost,
  COALESCE(SUM(cnr.on_duration_ms), 0) AS on_duration_ms,
  COALESCE(SUM(cnr.idle_duration_ms), 0) AS idle_duration_ms,
  SUM(cnr.aux_during_idle_ms) AS aux_during_idle_ms,
  COALESCE(SUM(cnr.electric_distance_traveled_m), 0) AS electric_distance_traveled_m,
  COALESCE(SUM(cnr.energy_regen_kwh), 0) AS energy_regen_kwh
FROM
  fuel_energy_efficiency_report.combined_new_rows_v3 cnr
WHERE cnr.driver_id != 0
AND date >= ${start_date}
AND date < ${end_date}
GROUP BY org_id, driver_id, interval_start, interval_end, date
),

-- 1 meter = 0.0006213711922 miles
-- 1 MPGEe = 33,705 watt-hours per mile
-- 1 ml = 0.000264172 gallons
with_mpg AS (
SELECT
  *,
  CASE -- we need mpg calculations to know which hourly data points to throw out
    WHEN fuel_consumed_ml > 0 and energy_consumed_kwh > 0
      THEN electric_distance_traveled_m / distance_traveled_m * 33705 / (energy_consumed_kwh * 1000 / (electric_distance_traveled_m * 0.0006213711922)) +
          (1 - electric_distance_traveled_m / distance_traveled_m) * (distance_traveled_m - electric_distance_traveled_m) * 0.0006213711922 / (fuel_consumed_ml * 0.000264172)
    WHEN fuel_consumed_ml = 0 AND energy_consumed_kwh > 0
      THEN 33705 / (energy_consumed_kwh * 1000 / (electric_distance_traveled_m * 0.0006213711922))
    WHEN fuel_consumed_ml > 0 AND energy_consumed_kwh = 0
      THEN distance_traveled_m * 0.0006213711922 / (fuel_consumed_ml * 0.000264172)
    ELSE 0
  END weighted_mpge,
  CASE -- record energy consumed value to know which threshold to use
    WHEN energy_consumed_kwh > 0 THEN 'Y'
    ELSE 'N'
  END has_used_energy,
  CASE
    WHEN fuel_consumed_ml = 0 AND energy_consumed_kwh = 0 THEN 'N'
    ELSE 'Y'
  END has_used_fuel_or_energy
FROM aggregated_vehicle_driver_rows
WHERE date >= ${start_date}
AND date < ${end_date}
),

with_corrected AS (
SELECT
    date,
    org_id,
    object_id,
    object_type,
    interval_start,
    interval_end,
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
      ELSE energy_consumed_kwh
    END energy_consumed_kwh,
    energy_consumed_kwh AS energy_consumed_kwh_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE distance_traveled_m
    END distance_traveled_m,
    distance_traveled_m AS distance_traveled_m_original,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE custom_fuel_cost
    END custom_fuel_cost,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE dynamic_fuel_cost
    END dynamic_fuel_cost,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE custom_energy_cost
    END custom_energy_cost,
    CASE
      WHEN has_used_fuel_or_energy = 'N' THEN 0
      WHEN has_used_energy = 'Y' AND weighted_mpge > 200 THEN 0
      WHEN has_used_energy = 'N' AND weighted_mpge > 65 THEN 0
      ELSE dynamic_energy_cost
    END dynamic_energy_cost,
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
)

SELECT
  date,
  org_id,
  object_id,
  object_type,
  interval_start,
  interval_end,
  fuel_consumed_ml,
  fuel_consumed_ml_original,
  energy_consumed_kwh,
  energy_consumed_kwh_original,
  distance_traveled_m,
  distance_traveled_m_original,
  -- GPS and Odo distances do not exist in the v3 pipeline, so set them to null
  -- However, we must cast them to the right type; setting as NULL will make the
  -- column type null as well.
  CAST(NULL as double) AS distance_traveled_m_gps,
  CAST(NULL as double) AS distance_traveled_m_gps_original,
  CAST(NULL as bigint) AS distance_traveled_m_odo,
  CAST(NULL as bigint) AS distance_traveled_m_odo_original,
  custom_fuel_cost,
  dynamic_fuel_cost,
  custom_energy_cost,
  dynamic_energy_cost,
  on_duration_ms,
  idle_duration_ms,
  aux_during_idle_ms,
  electric_distance_traveled_m,
  electric_distance_traveled_m_original,
  energy_regen_kwh,
  energy_regen_kwh_original
FROM with_corrected
WHERE date >= ${start_date}
  AND date < ${end_date}
  -- Filter out rows with all zeroes to reduce the number of rows being written.
  AND (fuel_consumed_ml + fuel_consumed_ml_original
    + energy_consumed_kwh + energy_consumed_kwh_original
    + distance_traveled_m + distance_traveled_m_original
    + custom_fuel_cost + dynamic_fuel_cost + custom_energy_cost + dynamic_energy_cost
    + on_duration_ms + idle_duration_ms
    + electric_distance_traveled_m + electric_distance_traveled_m_original
    + energy_regen_kwh + energy_regen_kwh_original
    + COALESCE(aux_during_idle_ms, 0)
    ) != 0
