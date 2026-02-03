/*
  TL;DR - Assignments starts when a trip starts and ends when the next trip starts unless
  the gap between trips is 6+ hours and in that case the difference between trips is split
  equally between assigments.

  In order to determine periods of driver device assignments we use trips data.
  We fetch trips in range along with the last and next trip outside of the range for each device.
  During manipulation of data we want to keep the original data until we arrive at a conclusion
    - this is important for driver_id so that we distinguish between zero and null assignments
    - this is important for end_time so that we don't adjust default values.
  Once we have trips we add context to each trip about the last and next trip.
  Trips are not true representations of driver device assignments, a driver may enter a vehicle and
    begin idling however the trip and hence assignment won't start until the vehicle starts moving.
  To account for this we make adjustments to the assigments that we can determine from trips.
  If there is more than 6 hours between trips then we adjust the trip start and end times by half
    of the difference between the trips times.
  Without these adjustments vehicles would have at least 1 minute before a trip starts where
    data is attributed to the previous driver (this is less of a concern where all drivers
    behave in a consistent manner as each driver would have slightly incorrect data).
  The likely situation we want to account for is drivers parking and another driver picking up
    a vehicle, 6 hours is a conservative real world expectation for this scenario and should
    mitigate the risk of reported data whilst a vehicle is running but not moving
    (for example in a traffic jam).
  We then take these adjusted trip times and generate driver device assignment periods.
  These adjustments of driver assignments are a temporary measure until Samsara develops a more
    sustainable and common driver device assignment methodology.
  There is still a risk of these assignments resulting in misattributed data. For example a
    driver could park, another driver could take the vehicle after 3 hours, idle for 10 minutes
    and then begin moving. In this scenario the 10 minutes of idling time would be attributed
    to the first driver as the driver assignment for the first driver would not end until the
    vehicle (with the second driver) had started moving.
*/
WITH trips AS (
  -- Trips between start_date and end_date
  SELECT
    DATE(date) AS date,
    org_id,
    device_id,
    driver_id_exclude_0_drivers as driver_id,
    proto.start.time AS start_time,
    proto.end.time AS end_time
  FROM trips2db_shards.trips
    WHERE DATE(date) >= ${start_date}
    AND DATE(date) < ${end_date}
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
    AND org_id != 562949953421343
    AND version = 101
  UNION

  -- The last trip before start_date for each org_id/device_id combination
  SELECT
    DATE(MAX(date)) AS date,
    org_id,
    device_id,
    MAX_BY(driver_id_exclude_0_drivers, proto.start.time) AS driver_id,
    MAX(proto.start.time) AS start_time,
    MAX_BY(proto.end.time, proto.start.time) AS end_time
  FROM trips2db_shards.trips
  WHERE DATE(date) < ${start_date}
    -- Only carry forward driver assignments from within 30 days of start_date
    AND DATE(date) >= DATE_SUB(${start_date}, 30)
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
    AND org_id != 562949953421343
    AND version = 101
  GROUP BY org_id, device_id
  UNION

  -- The next trip after end_date for each org_id/device_id combination
  SELECT
    DATE(MIN(date)) AS date,
    org_id,
    device_id,
    MIN_BY(driver_id_exclude_0_drivers, proto.start.time) AS driver_id,
    MIN(proto.start.time) AS start_time,
    MIN_BY(proto.end.time, proto.start.time) AS end_time
  FROM trips2db_shards.trips
  WHERE DATE(date) >= ${end_date}
    -- Only consider driver assignments after 30 days of end_date
    AND DATE(date) < DATE_ADD(${end_date}, 30)
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
    AND org_id != 562949953421343
    AND version = 101
  GROUP BY org_id, device_id
), trips_with_context AS (
  -- Trips including the previous trip end time and the next trip start time if available
  SELECT
    date,
    org_id,
    device_id,
    driver_id,
    LAG(end_time) OVER (PARTITION BY org_id, device_id ORDER BY start_time) AS last_end_time,
    start_time,
    end_time,
    LEAD(start_time) OVER (PARTITION BY org_id, device_id ORDER BY start_time) AS next_start_time
  FROM trips
), trips_with_adjustments AS (
  -- Divide the difference between trips where the gap is more than 6 hours
  SELECT
    date,
    org_id,
    device_id,
    driver_id,
    CAST(IF (last_end_time IS NOT NULL AND start_time - last_end_time > 6*3600000, start_time - ((start_time - last_end_time) / 2), start_time) AS BIGINT) AS start_time,
    CAST(IF (next_start_time IS NOT NULL AND next_start_time - end_time > 6*3600000, end_time + ((next_start_time - end_time) / 2), end_time) AS BIGINT) AS end_time
    FROM trips_with_context
),

-- Driver assignments begin when a trip begins and end when the next one starts.
driver_assignments AS (
  SELECT
    date,
    COALESCE(driver_id, 0) AS driver_id,
    org_id,
    device_id,
    start_time,
    end_time
  FROM (
    SELECT
      date,
      org_id,
      device_id,
      driver_id,
      start_time,
      COALESCE(
        LEAD(start_time) OVER (PARTITION BY org_id, device_id ORDER BY start_time),
        CAST(unix_timestamp(CAST (${end_date} AS TIMESTAMP)) * 1000 AS BIGINT)
      ) AS end_time
    FROM trips_with_adjustments
  )
  -- Remove any driver assignments that start after end_date.
  -- However, keep assignments that start before start_date
  -- for now because they may overlap start_date. These will
  -- be dealt with below.
  WHERE TO_DATE(FROM_UNIXTIME(start_time / 1000)) < ${end_date}
),

-- Split assignments that overlap a day
-- This way the output will always fit cleanly with the report period,
-- which allows subsequent runs to cleanly overwrite data.
driver_assignments_split_by_day AS (
  SELECT *
  FROM (
    SELECT
      -- Make sure date is consistent with the subsegment start time
      TO_DATE(FROM_UNIXTIME(start_time / 1000)) AS date,
      driver_id,
      org_id,
      device_id,
      start_time,
      end_time
    FROM (
      SELECT
        driver_id,
        org_id,
        device_id,
        GREATEST(start_time, CAST(UNIX_TIMESTAMP(day_start) * 1000 AS BIGINT)) AS start_time,
        LEAST(end_time, CAST(UNIX_TIMESTAMP(day_start + INTERVAL 1 DAY) * 1000 AS BIGINT)) AS end_time
      FROM (
        SELECT
          *,
          EXPLODE(
            SEQUENCE(
              DATE_TRUNC('day', FROM_UNIXTIME(start_time/1000)),
              DATE_TRUNC('day', FROM_UNIXTIME((end_time-1)/1000)),
              INTERVAL 1 day
            )
          ) AS day_start
        FROM driver_assignments
      )
    )
  )
  -- Filter by date again to exclude the split segments that are outside the report period
  WHERE date >= ${start_date}
    AND date < ${end_date}
)

SELECT da.*
FROM driver_assignments_split_by_day da
-- Exclude assignments for trailers to avoid double counting driver data.
JOIN (
  SELECT org_id, id
  FROM productsdb.devices
  WHERE asset_type <> 1
) d
 ON da.org_id = d.org_id
AND da.device_id = d.id
