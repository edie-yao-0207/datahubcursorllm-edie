SELECT
  org_id,
  date,
  driver_id AS user_id,
  dvir_id
FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_telematics.db/fct_dvirs`
WHERE inspection_type in ("pretrip", "posttrip")
