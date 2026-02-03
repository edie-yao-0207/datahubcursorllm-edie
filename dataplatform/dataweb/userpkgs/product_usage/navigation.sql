SELECT
  org_id,
  date,
  driver_id AS user_id,
  nav_session_uuid
FROM datastreams_history.mobile_nav_routing_events
WHERE
  org_id IS NOT NULL
  AND event_type = 'NavState-startingNavigation'
  AND driver_id != 0
  AND driver_id IS NOT NULL
