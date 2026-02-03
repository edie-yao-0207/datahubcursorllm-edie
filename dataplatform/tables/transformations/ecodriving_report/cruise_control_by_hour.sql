SELECT
  cc.date,
  cc.org_id,
  cc.object_id AS device_id,
  -- Combine NULL and ZERO driver assignments to be equivalent.
  -- This ensures that we don't lose data for unassigned periods.
  -- This also ensures that we don't run into duplicate rows on merge downstream.
  COALESCE(da.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  SUM(cruise_control_ms) AS cruise_control_ms
FROM objectstat_diffs.cruise_control AS cc
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS da -- Use same driver_assignment as FEER for consistency
 ON da.org_id = cc.org_id
 AND da.device_id = cc.object_id
 AND da.end_time >= CAST(unix_timestamp(CAST (${start_date} AS TIMESTAMP)) * 1000 AS BIGINT)
 AND cc.time BETWEEN da.start_time AND da.end_time
WHERE cc.cruise_control_ms IS NOT NULL
AND cc.cruise_control_ms > 0 -- Ignore resets.
AND cc.date >= ${start_date}
AND cc.date < ${end_date}
GROUP BY cc.date, cc.org_id, cc.object_id, COALESCE(da.driver_id, 0), WINDOW

