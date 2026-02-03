WITH location_when_engine_on AS (
  SELECT
    l.*
  FROM
    kinesisstats_window.location l
  JOIN engine_state.intervals i
    ON l.date = i.date
    AND l.org_id = i.org_id
    AND l.device_id = i.object_id
    AND l.time >= i.start_ms
    AND l.time < i.end_ms
    AND i.state = "ON" -- considering only engine ON data.
  WHERE
    l.date >= '2024-01-01' -- topography related data will be available from 2024 onwards.
    AND l.date >= ${start_date}
    AND l.date < ${end_date}
    AND i.date >= '2024-01-01' -- both tables are partitioned on date, so filtering date range for both will help in reducing the amount of data scanned and in turn reduce processing time.
    AND i.date >= ${start_date}
    AND i.date < ${end_date}
),
relevant_location_data AS (
  SELECT
    org_id,
    device_id,
    time,
    date,
    value.altitude_meters AS altitude_meters,
    LAG(value.altitude_meters, 6) OVER (
      PARTITION BY org_id,
      device_id
      ORDER BY
        time
    ) AS altitude_6_rows_back_meters, -- we get kinesisstats location every 5 seconds. getting altitude data points at 30 seconds interval.
    time - previous.time as duration_ms,
    time - LAG(time, 6) OVER (
      PARTITION BY org_id,
      device_id
      ORDER BY
        time
    ) AS duration_6_rows_back_ms -- we require duration to validate previous altitude data point is at 30 seconds interval.
  FROM
    location_when_engine_on
  WHERE
    value.gps_speed_meters_per_second >= 2.2352 -- minimum speed threshold is considered to be 5 miles/h, that is, 2.2352 m/s.
),
filtered_location_data as (
  SELECT
    org_id,
    device_id,
    time,
    date,
    altitude_meters,
    altitude_6_rows_back_meters,
    duration_ms
  FROM
    relevant_location_data
  WHERE
    duration_ms <= 10 * 1000 -- filter out huge time gaps between location data points.
    AND duration_6_rows_back_ms >= 30 * 1000 -- 30 seconds time interval has better accuracy to calculate grade percentage.
    AND duration_6_rows_back_ms <= 60 * 1000 -- specifying a threshold to filter out large gaps between trips.
    AND altitude_meters IS NOT NULL -- filter out the location data points with null altitude.
    AND altitude_6_rows_back_meters IS NOT NULL
),
location_with_cumulative_distance AS (
  SELECT
    ld.*,
    COALESCE(
      SUM(td.distance) OVER (
        PARTITION BY td.org_id,
        td.device_id
        ORDER BY
          td.time_ms ROWS BETWEEN 5 PRECEDING
          AND CURRENT ROW
      ),
    0) AS distance -- default to 0 if no matching total_distance row is found.
  FROM
    filtered_location_data ld
  LEFT JOIN canonical_distance.total_distance td
    ON ld.org_id = td.org_id
    AND ld.device_id = td.device_id
    AND ld.date = td.date
    AND ld.time = td.time_ms
  WHERE
    td.distance > 11 -- vehicle driving at at least 2.2352 m/s over 5 seconds should cover at least 11m distance.
    AND td.date >= ${start_date} -- table is partitioned on date, so filtering date range will help in reducing the amount of data scanned and in turn reduce processing time.
    AND td.date < ${end_date}
),
distance_with_grade_percentage AS (
  SELECT
    org_id,
    device_id,
    DATE_TRUNC('hour', FROM_UNIXTIME(time / 1000)) AS interval_start,
    time,
    date,
    duration_ms,
    -- grade_percentage = (vertical_rise / horizontal_distance_traveled) * 100.
    -- where vertical_rise = (altitude_meters - previous_altitude_meters) and horizontal_distance_traveled = sqrt(distance^2 - vertical_rise^2).
    (altitude_meters - altitude_6_rows_back_meters) / (SQRT(POWER(distance, 2) - POWER(altitude_meters - altitude_6_rows_back_meters, 2))) * 100 AS grade_percentage
  FROM
    location_with_cumulative_distance
  WHERE
    distance >= 67.056 -- vehicle driving at at least 2.2352 m/s over 30 seconds should cover at least 67.056m distance.
    AND POWER(distance, 2) > POWER(altitude_meters - altitude_6_rows_back_meters, 2)
)

SELECT
  dwgp.org_id,
  dwgp.device_id,
  -- Combine NULL and ZERO driver assignments to be equivalent.
  -- This ensures that we don't lose data for unassigned periods.
  -- This also ensures that we don't run into duplicate rows on merge downstream.
  COALESCE(da.driver_id, 0) AS driver_id,
  dwgp.date,
  dwgp.interval_start,
  dwgp.interval_start + INTERVAL 1 HOUR AS interval_end,
  -- minimum grade percentage threshold is considered to be 2% on either inclines.
  SUM(CASE WHEN dwgp.grade_percentage > 2 THEN dwgp.duration_ms ELSE 0 END) AS uphill_duration_ms,
  SUM(CASE WHEN dwgp.grade_percentage < -2 THEN dwgp.duration_ms ELSE 0 END) AS downhill_duration_ms
FROM
  distance_with_grade_percentage dwgp
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS da -- Use same driver_assignment as FEER for consistency
 ON da.org_id = dwgp.org_id
 AND da.device_id = dwgp.device_id
 AND da.end_time >= CAST(unix_timestamp(CAST (${start_date} AS TIMESTAMP)) * 1000 AS BIGINT)
 AND dwgp.time BETWEEN da.start_time AND da.end_time
GROUP BY
  dwgp.date,
  dwgp.org_id,
  dwgp.device_id,
  interval_start,
  COALESCE(da.driver_id, 0)

