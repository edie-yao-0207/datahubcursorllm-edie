WITH support_data_snapshot AS (
  SELECT DISTINCT
    support.sam_number,
    support.ticket_id,
    support.product_group as product_area,
    support.ticket_status
  FROM samsara_zendesk.support_tickets support
),

support_pivoted_snapshot AS (
  SELECT * FROM ( 
    SELECT
      sam_number, 
      CASE 
        WHEN REPLACE(REPLACE(LOWER(product_area),' ', '_'), 'pg_', '') IN ('assets', 'backend_infrastructure', 'connected_admin', 'connected_driver', 'dashcam', 'eurofleet', 'fleet_app_ecosystem', 'fleet_management_other', 'hardware', 'industrial', 'multicam', 'non-product_related', 'platform', 'safety', 'telematics') THEN REPLACE(REPLACE(LOWER(product_area),' ', '_'), 'pg_', '')
        ELSE 'unknown'
      END AS product_area
    FROM support_data_snapshot
    WHERE LOWER(ticket_status) LIKE '%open%'
  )
  PIVOT (
    COUNT(1)
    for product_area IN ('assets', 'backend_infrastructure', 'connected_admin', 'connected_driver', 'dashcam', 'eurofleet', 'fleet_app_ecosystem', 'fleet_management_other', 'hardware', 'industrial', 'multicam', 'non-product_related', 'platform', 'safety', 'telematics', 'unknown')
  ) 
)

SELECT
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
  cs.unknown
FROM support_pivoted_snapshot cs
WHERE sam_number IS NOT NULL
