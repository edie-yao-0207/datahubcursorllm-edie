-- Sum energy consumption by (device, driver, hour).
SELECT
  ed.date,
  ed.org_id,
  ed.object_id,
  COALESCE(driver.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  SUM(energy_consumed_uwh) * CAST(1e-9 AS DECIMAL(9,9)) AS energy_consumed_kwh
FROM
  objectstat_diffs.energy AS ed
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS driver
 ON driver.org_id = ed.org_id
 AND driver.device_id = ed.object_id
 AND ed.time BETWEEN driver.start_time AND driver.end_time
WHERE ed.energy_consumed_uwh IS NOT NULL
AND ed.energy_consumed_uwh > 0 -- Ignore resets.
AND ed.date >= ${start_date}
AND ed.date < ${end_date}
GROUP BY ed.date, ed.org_id, ed.object_id, COALESCE(driver.driver_id, 0), WINDOW

