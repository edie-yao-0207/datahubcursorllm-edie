SELECT
  CAST(rr.org_id AS BIGINT) AS org_id,
  DATE_TRUNC('day', rr.created_at) AS date,
  rr.user_id AS user_id,
  CONCAT(rr.org_id, '_', rr.uuid) AS usage_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-reportconfigdb/reportconfigdb/report_runs_v0` rr
JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` o
  ON rr.org_id = o.id
WHERE
  rr.report_run_metadata.report_config IS NOT NULL
  AND rr.report_run_metadata.report_config.is_superuser_only = FALSE -- Exclude superuser-only reports
  AND rr.org_id IS NOT NULL
  AND rr.created_at IS NOT NULL
  -- Note: We are not filtering on rr.status (e.g., status=2 for 'Complete')
  -- to count all attempts that meet the above criteria, similar to the engineer's query.
  -- If only 'Complete' runs should be counted, add 'AND rr.status = 2' here.

UNION ALL

SELECT
  CAST(rr.org_id AS BIGINT) AS org_id,
  DATE_TRUNC('day', rr.created_at) AS date,
  rr.user_id AS user_id,
  CONCAT(rr.org_id, '_', rr.uuid) AS usage_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-reportconfig-shard-1db/reportconfigdb/report_runs_v0` rr
JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` o
  ON rr.org_id = o.id
WHERE
  rr.report_run_metadata.report_config IS NOT NULL
  AND rr.report_run_metadata.report_config.is_superuser_only = FALSE -- Exclude superuser-only reports
  AND rr.org_id IS NOT NULL
  AND rr.created_at IS NOT NULL
  -- Note: We are not filtering on rr.status (e.g., status=2 for 'Complete')
  -- to count all attempts that meet the above criteria, similar to the engineer's query.
  -- If only 'Complete' runs should be counted, add 'AND rr.status = 2' here.
