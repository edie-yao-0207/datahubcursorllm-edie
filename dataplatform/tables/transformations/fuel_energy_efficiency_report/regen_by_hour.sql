-- Sum regenerated energy by (device, driver, hour).
SELECT
  rd.date,
  rd.org_id,
  rd.object_id,
  COALESCE(driver.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  SUM(energy_regen_uwh) * CAST(1e-9 AS DECIMAL(9,9)) AS energy_regen_kwh
FROM objectstat_diffs.regen as rd
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS driver
  ON driver.org_id = rd.org_id
  AND driver.device_id = rd.object_id
  AND rd.time BETWEEN driver.start_time AND driver.end_time
WHERE energy_regen_uwh IS NOT NULL
AND rd.date >= ${start_date}
AND rd.date < ${end_date}
GROUP BY rd.date, rd.org_id, rd.object_id, COALESCE(driver.driver_id, 0), WINDOW

