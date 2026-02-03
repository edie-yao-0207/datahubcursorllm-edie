SELECT
  org_id,
  date,
  driver_id AS user_id,
  event_count
FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_silver.db/stg_driver_app_events`
WHERE driver_id != 0
