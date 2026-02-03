SELECT
  cd.date,
  cd.org_id,
  cd.object_id AS device_id,
  -- Combine NULL and ZERO driver assignments to be equivalent.
  -- This ensures that we don't lose data for unassigned periods.
  -- This also ensures that we don't run into duplicate rows on merge downstream.
  COALESCE(da.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  SUM(coasting_time_ms) AS coasting_time_ms
FROM objectstat_diffs.coasting AS cd
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS da -- Use same driver_assignment as FEER for consistency
 ON da.org_id = cd.org_id
 AND da.device_id = cd.object_id
 AND da.end_time >= CAST(unix_timestamp(CAST (${start_date} AS TIMESTAMP)) * 1000 AS BIGINT)
 AND cd.time BETWEEN da.start_time AND da.end_time
WHERE cd.coasting_time_ms IS NOT NULL
AND cd.coasting_time_ms > 0 -- Ignore resets.
AND cd.date >= ${start_date}
AND cd.date < ${end_date}
GROUP BY cd.date, cd.org_id, cd.object_id, COALESCE(da.driver_id, 0), WINDOW

