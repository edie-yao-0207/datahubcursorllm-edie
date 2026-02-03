WITH
comments_created_at AS (
  SELECT ticket_comment.ticket_id,
         created
  FROM samsara_zendesk.ticket_comment
  JOIN samsara_zendesk.user
    ON ticket_comment.user_id = user.id
  WHERE user.role IN ('admin', 'agent') AND public
  AND user_id NOT IN (
    SELECT submitter_id 
    FROM samsara_zendesk.ticket t1 
    WHERE t1.id = ticket_comment.ticket_id 
    )
),
ticket_first_responded_at AS (
  SELECT ticket_id,
         MIN(created) AS responded_at
  FROM comments_created_at
  GROUP BY ticket_id
),
ticket_first_responded_time AS (
  SELECT ticket.id AS ticket_id,
         ticket.created_at,
         MAX(ticket_first_responded_at.responded_at) AS responded_at
  FROM samsara_zendesk.ticket
  JOIN ticket_first_responded_at ON ticket.id = ticket_first_responded_at.ticket_id
  GROUP BY 1, 2
),
first_responded_time_1 AS (
  SELECT *,
          GREATEST(0, ( UNIX_TIMESTAMP(responded_at) - UNIX_TIMESTAMP(created_at))/60) as first_responded_time_in_calendar_minutes
  FROM ticket_first_responded_time
),
ticket_last_updated_at AS (
  SELECT ticket_id,
         MAX(updated) AS last_updated_at
  FROM samsara_zendesk.ticket_field_history
  GROUP BY 1
),
first_responded_time AS (
  SELECT first_responded_time_1.ticket_id,
         DATE(last_updated_at) AS last_updated_at,
         first_responded_time_in_calendar_minutes
  FROM first_responded_time_1 
  LEFT JOIN ticket_last_updated_at
  ON first_responded_time_1.ticket_id = ticket_last_updated_at.ticket_id
)
SELECT * 
FROM first_responded_time
WHERE last_updated_at >= ${start_date} 
AND last_updated_at < ${end_date}
