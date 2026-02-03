WITH support_data AS (
  SELECT DISTINCT
    support.date_ticket_created AS date,
    support.sam_number,
    support.ticket_id,
    support.product_group AS product_area,
    support.ticket_status
  FROM samsara_zendesk.support_tickets support
),

support_pivoted AS (
  SELECT * FROM (
    SELECT
      date,
      sam_number, 
      CASE 
        WHEN REPLACE(REPLACE(LOWER(product_area),' ', '_'), 'pg_', '') IN ('assets', 'backend_infrastructure', 'connected_admin', 'connected_driver', 'dashcam', 'eurofleet', 'fleet_app_ecosystem', 'fleet_management_other', 'hardware', 'industrial', 'multicam', 'non-product_related', 'platform', 'safety', 'telematics') THEN REPLACE(REPLACE(LOWER(product_area),' ', '_'), 'pg_', '')
        ELSE 'unknown'
      END AS product_area
    FROM support_data
  )
  PIVOT (
    COUNT(1)
    for product_area IN ('assets', 'backend_infrastructure', 'connected_admin', 'connected_driver', 'dashcam', 'eurofleet', 'fleet_app_ecosystem', 'fleet_management_other', 'hardware', 'industrial', 'multicam', 'non-product_related', 'platform', 'safety', 'telematics', 'unknown')
  ) 
),

ticket_and_orgs AS (
  SELECT 
    t1.id AS ticket_id, 
    REPLACE(t2.custom_sam_number_organization_, "-", "") AS sam_number, 
    t2.name AS organization_name 
  FROM samsara_zendesk.ticket t1 
  JOIN samsara_zendesk.organization t2 
  ON t1.organization_id = t2.id
),

full_resolution_times AS (
  SELECT 
    t1.ticket_id,
    t2.sam_number, 
    full_resolution_time_in_calendar_minutes
  FROM samsara_zendesk.full_resolution_times t1 
  JOIN ticket_and_orgs t2 
  ON t1.ticket_id = t2.ticket_id
),

solved_at_dates AS (
  SELECT 
    ticket_id,
    MAX(updated) AS solved_at
  FROM samsara_zendesk.ticket_field_history
  WHERE value = "solved"
  GROUP BY ticket_id 
),

sum_resolution_times_per_day_per_sam_number AS (
  SELECT 
    sam_number,
    DATE(solved_at) AS solved_at,
    SUM(full_resolution_time_in_calendar_minutes) AS time_til_resolution_minutes,
    COUNT(1) AS num_tickets_solved
  FROM full_resolution_times t1
  JOIN solved_at_dates t2
  ON t1.ticket_id = t2.ticket_id
  GROUP BY sam_number, DATE(solved_at)
)

SELECT
  cs.date,
  cs.sam_number,
  cs.assets,
  cs.backend_infrastructure,
  cs.connected_admin,
  cs.connected_driver,
  cs.dashcam,
  cs.eurofleet,
  cs.fleet_app_ecosystem,
  cs.`fleet_management_other` AS `fleet_management_-_other`,
  cs.hardware,
  cs.industrial,
  cs.multicam,
  cs.`non-product_related`,
  cs.platform,
  cs.safety,
  cs.telematics,
  cs.unknown,
  t2.time_til_resolution_minutes,
  t2.num_tickets_solved
FROM support_pivoted cs
LEFT JOIN sum_resolution_times_per_day_per_sam_number t2
ON cs.sam_number = t2.sam_number
AND cs.date = t2.solved_at
WHERE date >= ${start_date}
AND date < ${end_date}
AND cs.date IS NOT NULL 
AND cs.sam_number IS NOT NULL
