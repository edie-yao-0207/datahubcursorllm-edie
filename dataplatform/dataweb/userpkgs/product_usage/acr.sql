SELECT
  CAST(rr.org_id AS BIGINT) AS org_id,
  DATE_TRUNC('day', rr.created_at) AS date,
  rr.user_id AS user_id,
  CONCAT(rr.org_id, '_', rr.uuid) AS usage_id
FROM reportconfigdb_shards.report_runs rr
JOIN clouddb.organizations o
  ON rr.org_id = o.id
WHERE
  rr.report_run_metadata.report_config IS NOT NULL
  AND rr.report_run_metadata.report_config.is_superuser_only = FALSE -- Exclude superuser-only reports
  AND rr.org_id IS NOT NULL
  AND rr.created_at IS NOT NULL
