WITH
ticket_status_timeline AS (
  SELECT ticket_id,
         value AS ticket_status,
         updated AS status_start,
         COALESCE(lead(updated, 1) OVER ticket_status_timeline, CURRENT_TIMESTAMP()) AS status_end
  FROM samsara_zendesk.ticket_field_history
  WHERE field_name = 'status'
  WINDOW ticket_status_timeline AS (PARTITION BY ticket_id ORDER BY updated)
),
ticket_full_solved_time AS (
  SELECT *,
      GREATEST(0, ( UNIX_TIMESTAMP(status_end) - UNIX_TIMESTAMP(status_start))/60) AS raw_delta_in_minutes
  FROM ticket_status_timeline
),
calendar_minutes AS (
 SELECT ticket_id,
        ticket_status,
        IF(ticket_status IN ('pending'), raw_delta_in_minutes, 0) AS agent_wait_time_in_minutes,
        IF(ticket_status IN ('new', 'open', 'hold'), raw_delta_in_minutes, 0) AS requester_wait_time_in_minutes,
        IF(ticket_status IN ('hold'), raw_delta_in_minutes, 0) AS on_hold_time_in_minutes
 FROM ticket_full_solved_time
),
calendar_minutes_aggregated AS (
  SELECT ticket_id,
         SUM(agent_wait_time_in_minutes) AS agent_wait_time_in_calendar_minutes,
         SUM(requester_wait_time_in_minutes) AS requester_wait_time_in_calendar_minutes,
         SUM(on_hold_time_in_minutes) AS on_hold_time_in_calendar_minutes
  FROM calendar_minutes
  GROUP BY 1
),
ticket_last_updated_at AS (
  SELECT ticket_id,
         MAX(updated) AS last_updated_at
  FROM samsara_zendesk.ticket_field_history
  GROUP BY 1
),
wait_times AS (
  SELECT calendar_minutes_aggregated.ticket_id AS ticket_id,
         DATE(last_updated_at) AS last_updated_at,
         agent_wait_time_in_calendar_minutes,
         requester_wait_time_in_calendar_minutes,
         on_hold_time_in_calendar_minutes
  FROM calendar_minutes_aggregated 
  LEFT JOIN ticket_last_updated_at
  ON calendar_minutes_aggregated.ticket_id = ticket_last_updated_at.ticket_id
)
SELECT * 
FROM wait_times 
WHERE last_updated_at >= ${start_date} 
AND last_updated_at < ${end_date}
  
