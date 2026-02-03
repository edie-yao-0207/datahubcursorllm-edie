SELECT
  be.date,
  be.org_id,
  be.object_id AS device_id,
  -- Combine NULL and ZERO driver assignments to be equivalent.
  -- This ensures that we don't lose data for unassigned periods.
  -- This also ensures that we don't run into duplicate rows on merge downstream.
  COALESCE(da.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  SUM(brake_events) AS brake_events
FROM objectstat_diffs.brake_events AS be
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS da -- Use same driver_assignment as FEER for consistency
 ON da.org_id = be.org_id
 AND da.device_id = be.object_id
 AND da.end_time >= CAST(unix_timestamp(CAST (${start_date} AS TIMESTAMP)) * 1000 AS BIGINT)
 AND be.time BETWEEN da.start_time AND da.end_time
WHERE be.brake_events IS NOT NULL
AND be.brake_events > 0 -- Ignore resets.
AND be.date >= ${start_date}
AND be.date < ${end_date}
GROUP BY be.date, be.org_id, be.object_id, COALESCE(da.driver_id, 0), WINDOW

