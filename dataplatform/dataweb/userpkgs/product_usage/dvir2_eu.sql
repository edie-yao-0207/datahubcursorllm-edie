SELECT
  CAST(f.org_id AS BIGINT) AS org_id,
  DATE(f.server_created_at) AS date,
  f.created_by_polymorphic AS user_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_submissions_v0` f
WHERE
  f.status = 1 -- 1 represents completed
  AND f.product_type IN (10, 11) -- DVIR
  AND COALESCE(DATE(f.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid record

UNION ALL

SELECT
  CAST(f.org_id AS BIGINT) AS org_id,
  DATE(f.server_created_at) AS date,
  f.created_by_polymorphic AS user_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-forms-shard-1db/formsdb/form_submissions_v0` f
WHERE
  f.status = 1 -- 1 represents completed
  AND f.product_type IN (10, 11) -- DVIR
  AND COALESCE(DATE(f.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid record
