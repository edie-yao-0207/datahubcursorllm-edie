WITH intervals_with_assigned_driver AS (
  SELECT
    da.date,
    da.org_id,
    da.device_id,
    da.driver_id,
    da.start_time,
    da.end_time,
    ROW_NUMBER() OVER (
      PARTITION BY patfi.date,
        patfi.org_id,
        patfi.object_id,
        patfi.start_ms
      ORDER BY
        da.start_time ASC,
        (da.end_time - da.start_time) DESC -- Prefer longer assignments.
    ) AS row_number
  FROM
    engine_state_report.engine_state_pto_air_temp_fuel_intervals patfi
  INNER JOIN fuel_energy_efficiency_report.driver_assignments da -- Use same driver_assignment as FEER for consistency.
    ON da.date = patfi.date
    AND da.org_id = patfi.org_id
    AND da.device_id = patfi.object_id
    AND patfi.start_ms >= da.start_time
    AND patfi.start_ms < da.end_time -- Exclude end_time to avoid duplicate rows.
  WHERE
    patfi.date >= ${start_date}
    AND patfi.date < ${end_date}
    AND da.date >= ${start_date}
    AND da.date < ${end_date}
    AND da.driver_id IS NOT NULL
    AND da.driver_id != 0 -- Exclude unassigned drivers.
)
SELECT
  patfi.date,
  patfi.org_id,
  patfi.object_id,
  patfi.engine_state,
  patfi.productive_pto_state,
  patfi.fuel_consumed_ml,
  patfi.gaseous_fuel_consumed_grams,
  patfi.start_ms,
  patfi.end_ms,
  patfi.original_engine_state_interval_duration_ms,
  patfi.original_fuel_interval_duration_ms,
  patfi.original_gaseous_consumption_interval_duration_ms,
  patfi.original_pto_interval_duration_ms,
  patfi.avg_air_temp_c,
  -- Combine NULL and ZERO driver assignments to be equivalent.
  -- This ensures that we don't lose data for unassigned periods.
  -- This also ensures that we don't run into duplicate rows on merge downstream.
  COALESCE(iwad.driver_id, 0) AS driver_id
FROM
  engine_state_report.engine_state_pto_air_temp_fuel_intervals patfi
LEFT JOIN intervals_with_assigned_driver iwad
  ON iwad.date = patfi.date
  AND iwad.org_id = patfi.org_id
  AND iwad.device_id = patfi.object_id
  AND patfi.start_ms >= iwad.start_time
  AND patfi.start_ms < iwad.end_time
  AND iwad.row_number = 1 -- Select the first driver.
WHERE
  patfi.date >= ${start_date}
  AND patfi.date < ${end_date}
GROUP BY
  patfi.date,
  patfi.org_id,
  patfi.object_id,
  patfi.engine_state,
  patfi.productive_pto_state,
  patfi.fuel_consumed_ml,
  patfi.gaseous_fuel_consumed_grams,
  patfi.start_ms,
  patfi.end_ms,
  patfi.original_engine_state_interval_duration_ms,
  patfi.original_fuel_interval_duration_ms,
  patfi.original_gaseous_consumption_interval_duration_ms,
  patfi.original_pto_interval_duration_ms,
  patfi.avg_air_temp_c,
  COALESCE(iwad.driver_id, 0)
