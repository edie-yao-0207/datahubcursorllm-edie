WITH ticket_first_solved_at AS (
  SELECT ticket_id,
         MIN(ticket_field_history.updated) AS solved_at
  FROM samsara_zendesk.ticket_field_history
  WHERE ticket_field_history.value = 'solved'
  GROUP BY 1
),
ticket_first_solved_time AS (
  SELECT ticket.id AS ticket_id,
         ticket.created_at,
         MAX(ticket_first_solved_at.solved_at) AS solved_at
  FROM samsara_zendesk.ticket
  JOIN ticket_first_solved_at 
  ON ticket.id = ticket_first_solved_at.ticket_id
  GROUP BY 1, 2
),
first_resolution_time_1 AS (
  SELECT *,
          GREATEST(0, ( UNIX_TIMESTAMP(solved_at) - UNIX_TIMESTAMP(created_at))/60) AS first_resolution_time_in_calendar_minutes
  FROM ticket_first_solved_time
),
ticket_last_updated_at AS (
  SELECT ticket_id,
         MAX(updated) AS last_updated_at
  FROM samsara_zendesk.ticket_field_history
  GROUP BY 1
),
first_resolution_time AS (
  SELECT first_resolution_time_1.ticket_id,
         DATE(last_updated_at) AS last_updated_at,
         first_resolution_time_in_calendar_minutes
  FROM first_resolution_time_1 
  LEFT JOIN ticket_last_updated_at
  ON first_resolution_time_1.ticket_id = ticket_last_updated_at.ticket_id
)
SELECT * 
FROM first_resolution_time 
WHERE last_updated_at >= ${start_date} 
AND last_updated_at < ${end_date}
