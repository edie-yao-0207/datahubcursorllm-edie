WITH mobile_logs_and_trips_joined AS (
  SELECT
    t.date as date,
    t.start_ms as trip_start_ms,
    ml.timestamp as mobile_log_timestamp,
    t.org_id as org_id,
    t.device_id as device_id,
    t.driver_id as driver_id,
    ml.platform as mobile_platform,
    ml.os_version as mobile_os_version,
    GET_JSON_OBJECT(ml.app_info, "$$.bundleVersion") as mobile_bundle_version,
    -- Create rank column of matched rows by date, start_ms, org_id, device_id, driver_id and order by timestamp so we can get the lowest timestamp to the trip start
    RANK() OVER (PARTITION BY t.date, t.start_ms, t.org_id, t.device_id, t.driver_id ORDER BY ml.timestamp DESC) AS mobile_log_timestamp_rank
  FROM (
    SELECT * FROM trips2db_shards.trips WHERE date >= ${start_date} AND date < ${end_date} AND version = 101
  ) t
  -- Look up to two days before for a "DRIVER_TELEMATICS_LOCATION_TRACKING_STARTED" as some tracking
  -- sessions can be very long
  JOIN (
    SELECT * FROM datastreams.mobile_logs WHERE date >= DATE_ADD(${start_date}, -2) AND date < ${end_date} AND event_type = 'DRIVER_TELEMATICS_LOCATION_TRACKING_STARTED'
  ) ml
  -- JOIN on json_params from mobile logs to find the matching log for the trip
  ON GET_JSON_OBJECT(ml.json_params, "$$.deviceId") = t.device_id
  AND GET_JSON_OBJECT(ml.json_params, "$$.orgId") = t.org_id
  -- AND that the start_ms of the trip is equal or greater than the timestamp of the log
  AND t.start_ms >= (unix_timestamp(ml.timestamp)*1000)
)

SELECT
  DATE(date) as date,
  trip_start_ms,
  mobile_log_timestamp,
  org_id,
  device_id,
  driver_id,
  mobile_platform,
  mobile_os_version,
  mobile_bundle_version
FROM mobile_logs_and_trips_joined
-- Grab only the rows with the largest (aka highest rank) mobile_logs timestamp
-- This timestamp is the closest to the trip start so will be the most accurate mobile/os for that trip
WHERE mobile_log_timestamp_rank = 1
