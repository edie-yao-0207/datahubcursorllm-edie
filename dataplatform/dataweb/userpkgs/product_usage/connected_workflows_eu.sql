SELECT
  CAST(f.org_id AS BIGINT) AS org_id,
  DATE(f.server_created_at) AS date,
  COALESCE(f.submitted_by_polymorphic, f.assigned_to_polymorphic, f.created_by_polymorphic) AS user_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_submissions_v0` f
WHERE
  f.status = 1 -- 1 represents completed
  AND f.product_type = 1 -- Just Forms
  AND COALESCE(DATE(f.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid record
  AND f.created_by_polymorphic != 'user-1' --Filter out auto-generated events

UNION ALL

-- Usage is defined as orgs with at least one completed form submission from a non-internal user
-- There are many different Polymorphic users which may or may not have a value based on the product type
-- We chose a hierarchy of submitted, assigned, and created to find a user ID
SELECT
  CAST(f.org_id AS BIGINT) AS org_id,
  DATE(f.server_created_at) AS date,
  COALESCE(f.submitted_by_polymorphic, f.assigned_to_polymorphic, f.created_by_polymorphic) AS user_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-forms-shard-1db/formsdb/form_submissions_v0` f
WHERE
  f.status = 1 -- 1 represents completed
  AND f.product_type = 1 -- Just Forms
  AND COALESCE(DATE(f.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid record
  AND f.created_by_polymorphic != 'user-1' --Filter out auto-generated events
