SELECT
  aetol.date,
  aetol.org_id,
  aetol.object_id AS device_id,
  -- Combine NULL and ZERO driver assignments to be equivalent.
  -- This ensures that we don't lose data for unassigned periods.
  -- This also ensures that we don't run into duplicate rows on merge downstream.
  COALESCE(da.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  SUM(accel_engine_torque_over_limit_ms) AS accel_engine_torque_over_limit_ms
FROM objectstat_diffs.accel_engine_torque_over_limit AS aetol
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS da -- Use same driver_assignment as FEER for consistency
 ON da.org_id = aetol.org_id
 AND da.device_id = aetol.object_id
 AND da.end_time >= CAST(unix_timestamp(CAST (${start_date} AS TIMESTAMP)) * 1000 AS BIGINT)
 AND aetol.time BETWEEN da.start_time AND da.end_time
WHERE aetol.accel_engine_torque_over_limit_ms IS NOT NULL
AND aetol.accel_engine_torque_over_limit_ms > 0 -- Ignore resets.
AND aetol.date >= ${start_date}
AND aetol.date < ${end_date}
GROUP BY aetol.date, aetol.org_id, aetol.object_id, COALESCE(da.driver_id, 0), WINDOW

