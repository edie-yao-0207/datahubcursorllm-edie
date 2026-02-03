WITH daily_statuses AS (
  /*************************************************
  * We look to identify if calibration was running
  *  on a specific day, and include this information
  *  with our calibration state information
  *  we use this information at the day level
  *  since we believe there may be some issues
  *  with getting a calibration event on short trips
  **************************************************/
  SELECT
    DISTINCT object_id AS device_id,
    org_id,
    date
  FROM
    kinesisstats.osdimucalibrationstatus
),
trip_status_intervals AS (
  SELECT
    t.date,
    cl.device_id,
    cl.org_id,
    cl.status AS status,
    cl.source AS source,
    GREATEST(cl.start_ms, t.start_ms) AS start_ms,
    LEAST(cl.end_ms, t.end_ms) AS end_ms,
    t.start_ms AS trip_start_ms,
    t.end_ms AS trip_end_ms,
    cl.start_ms AS calibration_start_ms,
    cl.end_ms AS calibration_end_ms,
    ds.device_id IS NOT NULL AS algorithm_running_on_day
  FROM
    datascience.imu_calibration_intervals cl
  JOIN trips2db_shards.trips t ON
    t.device_id = cl.device_id
    AND t.org_id = cl.org_id
    AND cl.start_ms <= t.end_ms
    AND t.start_ms <= cl.end_ms
    AND t.version = 101
  LEFT JOIN daily_statuses ds ON
    ds.date = t.date
    AND ds.org_id = t.org_id
    AND ds.device_id = t.device_id
  WHERE
    t.date >= '2021-01-01'
)
SELECT
  date,
  device_id,
  org_id,
  status AS status,
  source AS source,
  start_ms,
  end_ms,
  LEAD(status) OVER(
    PARTITION BY
    org_id,
      device_id
    ORDER BY
      start_ms
  ) AS next_status,
  LEAD(source) OVER(
    PARTITION BY
      org_id,
      device_id
    ORDER BY
      start_ms
  ) AS next_source,
  trip_start_ms,
  trip_end_ms,
  calibration_start_ms,
  calibration_end_ms,
  algorithm_running_on_day
FROM
  trip_status_intervals
