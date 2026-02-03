WITH org_unassigned_segments AS (
  SELECT 
    date,
    org_id,
    COUNT(1) AS num_unassigned_segments
  FROM compliancedb_shards.driver_hos_logs
  WHERE 
    driver_id = 0 AND 
    status_code = 2 AND 
    (log_proto.event_record_status = 0 OR log_proto.event_record_status IS NULL) AND 
    log_type != 148514 -- autotagged segments
  GROUP BY 
    date,
    org_id
),

active_unassigned_logs AS (
  SELECT * 
  FROM compliancedb_shards.driver_hos_logs 
  WHERE 
    driver_id = 0 AND
    log_proto['active_to_inactive_changed_at_ms'] IS NULL
),

active_unassigned_logs_and_end_time AS (
  SELECT 
    *,
    log_at AS start_time,
    LEAD(log_at, 1) OVER (PARTITION BY org_id, vehicle_id ORDER BY log_at) AS end_time
  FROM active_unassigned_logs
),

active_unassigned_logs_and_duration AS (
  SELECT 
    *,
    (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time))/(60*60) AS unassigned_driver_time_hours
  FROM active_unassigned_logs_and_end_time
  WHERE status_code = 2
),

unassigned_driving_time AS (
  SELECT 
    org_id, 
    date, 
    SUM(unassigned_driver_time_hours) AS unassigned_driver_time_hours
  FROM active_unassigned_logs_and_duration
  GROUP BY
    org_id,
    date
)

SELECT
    to_date(us.date) as date,
    us.org_id,
    s.sam_number, 
    SUM(us.num_unassigned_segments) AS num_unassigned_segments,
    MAX(unassigned_driver_time_hours) AS unassigned_driver_time_hours
FROM org_unassigned_segments us
LEFT JOIN clouddb.org_sfdc_accounts o ON us.org_id = o.org_id
LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
LEFT JOIN unassigned_driving_time udt ON us.org_id = udt.org_id AND to_date(us.date) = udt.date
WHERE us.date >= ${start_date} AND
      us.date < ${end_date} AND
      us.date IS NOT NULL AND
      us.org_id IS NOT NULL
GROUP BY
    us.date,
    us.org_id,
    s.sam_number
