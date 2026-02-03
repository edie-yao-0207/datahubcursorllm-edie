-- Sum engine duration by (device, driver, engine state, hour),
-- by doing a range join between engine state intervals and driver intervals.
SELECT
  ed.date,
  ed.org_id,
  ed.object_id,
  COALESCE(driver.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(ed.start_ms / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(ed.start_ms / 1000), "1 hour").end AS interval_end,
  SUM(
    CASE
      WHEN ed.state = "ON" or ed.state = "IDLE" THEN
        LEAST(ed.end_ms, driver.end_time) - GREATEST(ed.start_ms, driver.start_time)
      ELSE 0
    END
  ) AS on_duration_ms,
  SUM(
    CASE
      WHEN ed.state = "IDLE" THEN
        LEAST(ed.end_ms, driver.end_time) - GREATEST(ed.start_ms, driver.start_time)
      ELSE 0
    END
  ) AS idle_duration_ms
FROM engine_state.intervals_by_hour AS ed
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS driver
 ON driver.org_id = ed.org_id
 AND driver.device_id = ed.object_id
 AND ed.start_ms <= driver.end_time AND ed.end_ms >= driver.start_time
WHERE ed.date >= ${start_date}
AND ed.date < ${end_date}
GROUP BY ed.date, ed.org_id, ed.object_id, COALESCE(driver.driver_id, 0), WINDOW
