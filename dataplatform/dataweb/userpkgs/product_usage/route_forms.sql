SELECT
  CAST(f.org_id AS BIGINT) AS org_id,
  DATE(f.server_created_at) AS date,
  COALESCE(f.submitted_by_polymorphic, f.assigned_to_polymorphic, f.created_by_polymorphic) AS user_id
FROM formsdb_shards.form_submissions f
WHERE
  f.status = 1 -- 1 represents completed
  AND f.product_type IN (2, 10, 11, 12, 13) -- Freemium product types
  AND COALESCE(DATE(f.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid record
  AND f.created_by_polymorphic != 'user-1' --Filter out auto-generated events
