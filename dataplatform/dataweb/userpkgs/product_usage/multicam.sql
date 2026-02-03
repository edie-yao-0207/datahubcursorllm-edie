SELECT
  org_id,
  device_id,
  date
FROM datamodel_core_silver.stg_device_activity_daily
WHERE product_name = 'AIM4'
  AND is_active
