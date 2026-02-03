WITH form_submissions_eu AS (
  SELECT
    org_id,
    assigned_at,
    assigned_to_polymorphic,
    form_template_uuid,
    server_deleted_at,
    product_type
  FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_submissions_v0`

  UNION ALL

  SELECT
    org_id,
    assigned_at,
    assigned_to_polymorphic,
    form_template_uuid,
    server_deleted_at,
    product_type
  FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-forms-shard-1db/formsdb/form_submissions_v0`
),
courses_eu AS (
    SELECT
        uuid,
        category_uuid,
        global_course_uuid,
        deleted_at_ms,
        title
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-training-shard-1db/trainingdb/courses_v0`

    UNION ALL

    SELECT
        uuid,
        category_uuid,
        global_course_uuid,
        deleted_at_ms,
        title
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-trainingdb/trainingdb/courses_v0`
),
categories_eu AS (
    SELECT
        uuid,
        label
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-training-shard-1db/trainingdb/categories_v0`

    UNION ALL

    SELECT
        uuid,
        label
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-trainingdb/trainingdb/categories_v0`
)
SELECT
  CAST(f.org_id AS BIGINT) AS org_id,
  DATE(f.assigned_at) AS date,
  f.assigned_to_polymorphic AS user_id
FROM form_submissions_eu f
JOIN courses_eu c
  ON f.form_template_uuid = c.uuid
JOIN categories_eu ca
  ON c.category_uuid = ca.uuid
WHERE
  f.assigned_at IS NOT NULL --assigned
  AND COALESCE(DATE(f.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid record
  AND product_type = 3 -- training
  AND (
    c.global_course_uuid IS NULL
    OR TRIM(ca.label) NOT IN (
      'Driver Safety - Shorts',
      'Samsara Driver App',
      'New Samsara Driver App',
      'Samsara Intro',
      'Announcements'
    )
  )
  AND COALESCE(DATE(FROM_UNIXTIME(c.deleted_at_ms / 1000)), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid records
