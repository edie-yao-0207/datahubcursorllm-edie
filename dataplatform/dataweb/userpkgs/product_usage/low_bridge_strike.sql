SELECT
  org_id,
  DATE(_timestamp) AS date
FROM dispatchdb_shards.dispatch_org_settings
WHERE
  setting_type = 11 --Low bridge strike
  AND value = 1
