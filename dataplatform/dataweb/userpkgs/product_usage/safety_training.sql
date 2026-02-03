SELECT
  CAST(f.org_id AS BIGINT) AS org_id,
  DATE(f.assigned_at) AS date,
  f.assigned_to_polymorphic AS user_id
FROM formsdb_shards.form_submissions f
JOIN trainingdb_shards.courses c
  ON f.form_template_uuid = c.uuid
JOIN trainingdb_shards.categories ca
  ON c.category_uuid = ca.uuid
WHERE
  f.assigned_at IS NOT NULL --assigned
  AND COALESCE(DATE(f.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --record is still valid
  AND f.product_type = 3 -- training
  AND COALESCE(DATE(FROM_UNIXTIME(c.deleted_at_ms / 1000)), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid records
  AND (
    TRIM(ca.label) IN (
      'Driver Safety - Shorts'
    )
    AND c.global_course_uuid IS NOT NULL
  )
