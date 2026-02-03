SELECT
  org_id,
  date,
  driver_id AS user_id
FROM datamodel_telematics_silver.stg_hos_logs
WHERE is_assigned
