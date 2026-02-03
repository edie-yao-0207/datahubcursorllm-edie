SELECT
  org_id,
  DATE(_timestamp) AS date
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-dispatchdb/dispatchdb/dispatch_org_settings_v0`
WHERE
  setting_type = 11 --Low bridge strike
  AND value = 1

UNION ALL

SELECT
  org_id,
  DATE(_timestamp) AS date
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-dispatch-shard-1db/dispatchdb/dispatch_org_settings_v0`
WHERE
  setting_type = 11 --Low bridge strike
  AND value = 1
