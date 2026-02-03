SELECT
  org_id,
  date,
  driver_id AS user_id,
  dvir_id
FROM datamodel_telematics.fct_dvirs
WHERE inspection_type in ("pretrip", "posttrip")
