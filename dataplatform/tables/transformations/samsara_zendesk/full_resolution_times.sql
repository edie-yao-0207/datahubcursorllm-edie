WITH ticket_last_updated_at AS (
  SELECT ticket_id,
         MAX(updated) AS last_updated_at
  FROM samsara_zendesk.ticket_field_history
  GROUP BY 1
),
merged_tickets AS (
  SELECT
    id AS parent_ticket,
    EXPLODE(SPLIT(SUBSTR(merged_ticket_ids, 2, LENGTH(merged_ticket_ids) - 2) , ",")) AS child_ticket
  FROM samsara_zendesk.ticket
  WHERE length(merged_ticket_ids) > 2
),
child_ticket_full_solved_at AS (
  SELECT
    child_ticket AS ticket_id,
    last_updated_at AS solved_at
  FROM merged_tickets t1
  LEFT JOIN ticket_last_updated_at t2
  ON t1.child_ticket = t2.ticket_id
),
parent_ticket_full_solved_at AS (
  SELECT
    ticket_id,
    MAX(ticket_field_history.updated) AS solved_at
  FROM samsara_zendesk.ticket_field_history
  WHERE ticket_field_history.value = 'solved'
  GROUP BY 1
),
ticket_full_solved_at_1 AS (
  SELECT *
  FROM parent_ticket_full_solved_at
  UNION
  SELECT *
  FROM child_ticket_full_solved_at
),
ticket_full_solved_at AS (
  SELECT *
  FROM ticket_full_solved_at_1 t1
  WHERE 'open' NOT IN (
    SELECT t2.value
    FROM samsara_zendesk.ticket_field_history t2
    WHERE t1.ticket_id = t2.ticket_id
    AND t2.updated > t1.solved_at
    AND t2.value is not null
    )
),
ticket_full_solved_time AS (
  SELECT ticket.id AS ticket_id,
         ticket.created_at,
         MAX(ticket_full_solved_at.solved_at) AS solved_at
  FROM samsara_zendesk.ticket
  JOIN ticket_full_solved_at ON ticket.id = ticket_full_solved_at.ticket_id
  GROUP BY 1, 2
),
full_resolution_time_1 AS (
  SELECT *,
          GREATEST(0, ( UNIX_TIMESTAMP(solved_at) - UNIX_TIMESTAMP(created_at))/60) AS full_resolution_time_in_calendar_minutes
  FROM ticket_full_solved_time
),
full_resolution_time AS (
  SELECT full_resolution_time_1.ticket_id,
         DATE(last_updated_at) AS last_updated_at,
         full_resolution_time_in_calendar_minutes
  FROM full_resolution_time_1
  LEFT JOIN ticket_last_updated_at
  ON full_resolution_time_1.ticket_id = ticket_last_updated_at.ticket_id
)
SELECT *
FROM full_resolution_time
WHERE last_updated_at >= ${start_date}
AND last_updated_at < ${end_date}
