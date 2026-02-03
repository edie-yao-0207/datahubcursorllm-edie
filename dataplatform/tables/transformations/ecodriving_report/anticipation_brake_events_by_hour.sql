SELECT
  abe.date,
  abe.org_id,
  abe.object_id AS device_id,
  -- Combine NULL and ZERO driver assignments to be equivalent.
  -- This ensures that we don't lose data for unassigned periods.
  -- This also ensures that we don't run into duplicate rows on merge downstream.
  COALESCE(da.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  SUM(anticipation_brake_events) AS anticipation_brake_events
FROM objectstat_diffs.anticipation_brake_events AS abe
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS da -- Use same driver_assignment as FEER for consistency
 ON da.org_id = abe.org_id
 AND da.device_id = abe.object_id
 AND da.end_time >= CAST(unix_timestamp(CAST (${start_date} AS TIMESTAMP)) * 1000 AS BIGINT)
 AND abe.time BETWEEN da.start_time AND da.end_time
WHERE abe.anticipation_brake_events IS NOT NULL
AND abe.anticipation_brake_events > 0 -- Ignore resets.
AND abe.date >= ${start_date}
AND abe.date < ${end_date}
GROUP BY abe.date, abe.org_id, abe.object_id, COALESCE(da.driver_id, 0), WINDOW

