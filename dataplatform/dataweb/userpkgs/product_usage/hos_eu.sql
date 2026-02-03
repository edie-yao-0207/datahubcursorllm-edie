SELECT
  org_id,
  date,
  driver_id AS user_id
FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_telematics_silver.db/stg_hos_logs`
WHERE is_assigned
