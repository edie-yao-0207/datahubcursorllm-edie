SELECT
  org_id,
  date,
  driver_id AS user_id,
  event_count
FROM datamodel_platform_silver.stg_driver_app_events
WHERE driver_id != 0
