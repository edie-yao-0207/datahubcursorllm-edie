SELECT
  org_id,
  device_id,
  date
FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_silver.db/stg_device_activity_daily`
WHERE product_name = 'AIM4'
  AND is_active
