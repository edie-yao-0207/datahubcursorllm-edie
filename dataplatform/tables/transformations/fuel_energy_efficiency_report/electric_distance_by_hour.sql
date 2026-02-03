-- Device can report EV distance in either meters or kilometers.
-- Sum electric power distance in meters by (device, driver, hour).
WITH electric_distance_by_hour_m AS (
SELECT
  odev.date,
  odev.org_id,
  odev.object_id,
  COALESCE(driver.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  SUM(distance_traveled_m) AS distance_traveled_m
FROM objectstat_diffs.odometer_ev_m AS odev
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS driver
 ON driver.org_id = odev.org_id
 AND driver.device_id = odev.object_id
 AND odev.time BETWEEN driver.start_time AND driver.end_time
WHERE odev.distance_traveled_m IS NOT NULL
AND odev.distance_traveled_m > 0
AND odev.date >= ${start_date}
AND odev.date < ${end_date}
GROUP BY odev.date, odev.org_id, odev.object_id, COALESCE(driver.driver_id, 0), WINDOW
),

-- Sum electric power distance in KM by (device, driver, hour).
electric_distance_by_hour_km AS (
SELECT
  odevkm.date,
  odevkm.org_id,
  odevkm.object_id,
  COALESCE(driver.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  SUM(distance_traveled_km) AS distance_traveled_km
FROM objectstat_diffs.odometer_ev_km AS odevkm
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS driver
 ON driver.org_id = odevkm.org_id
 AND driver.device_id = odevkm.object_id
 AND odevkm.time BETWEEN driver.start_time AND driver.end_time
WHERE odevkm.distance_traveled_km IS NOT NULL
AND odevkm.distance_traveled_km > 0
AND odevkm.date >= ${start_date}
AND odevkm.date < ${end_date}
GROUP BY odevkm.date, odevkm.org_id, odevkm.object_id, COALESCE(driver.driver_id, 0), WINDOW
)

-- Use high resolution data if available, and fallback to kilometers otherwise.
SELECT
  COALESCE(evm.date, evkm.date) AS date,
  COALESCE(evm.org_id, evkm.org_id) AS org_id,
  COALESCE(evm.object_id, evkm.object_id) AS object_id,
  COALESCE(evm.driver_id, evkm.driver_id, 0) AS driver_id,
  COALESCE(evm.interval_start, evkm.interval_start) AS interval_start,
  COALESCE(evm.interval_end, evkm.interval_end) AS interval_end,
  COALESCE(NULLIF(evm.distance_traveled_m, 0), evkm.distance_traveled_km * 1000) AS electric_distance_traveled_m_odo
FROM electric_distance_by_hour_m AS evm
FULL OUTER JOIN electric_distance_by_hour_km AS evkm
ON evm.org_id = evkm.org_id
AND evm.object_id = evkm.object_id
AND evm.driver_id = evkm.driver_id
AND evm.interval_start = evkm.interval_start
WHERE COALESCE(evm.date, evkm.date) >= ${start_date}
AND COALESCE(evm.date, evkm.date) < ${end_date}
ORDER BY object_id ASC, interval_start ASC
