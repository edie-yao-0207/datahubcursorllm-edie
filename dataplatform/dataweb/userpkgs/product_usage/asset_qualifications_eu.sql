WITH form_templates_eu AS (
  SELECT
    org_id,
    uuid,
    product_type,
    server_deleted_at,
    current_template_revision_uuid
  FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_templates_v0`

  UNION ALL

  SELECT
    org_id,
    uuid,
    product_type,
    server_deleted_at,
    current_template_revision_uuid
  FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-forms-shard-1db/formsdb/form_templates_v0`
),
form_template_revisions_eu AS (
  SELECT
    uuid
  FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_template_revisions_v0`

  UNION ALL

  SELECT
    uuid
  FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-forms-shard-1db/formsdb/form_template_revisions_v0`
),
form_template_revision_contents_eu AS (
  SELECT
    form_template_revision_uuid,
    proto
  FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_template_revision_contents_v1`

  UNION ALL

  SELECT
    form_template_revision_uuid,
    proto
  FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-forms-shard-1db/formsdb/form_template_revision_contents_v1`
),
qtypes_eu AS (
  -- Gets qualification type for each qualification event
SELECT
    ft.org_id,
    ft.uuid,
    SIZE(
        FILTER(
            ftrc.proto.field_definitions, fd -> fd.person_field_definition.common.key = 'ownerEntity'
        )
    ) > 0 AS is_worker_type,
    SIZE(
        FILTER(
            ftrc.proto.field_definitions, fd -> fd.asset_field_definition.common.key = 'ownerEntity'
        )
    ) > 0 AS is_asset_type
  FROM form_templates_eu ft
  LEFT JOIN form_template_revisions_eu ftr
    ON ftr.uuid = ft.current_template_revision_uuid
  LEFT JOIN form_template_revision_contents_eu ftrc
    ON ftr.uuid = ftrc.form_template_revision_uuid
  WHERE
    ft.product_type = 8
    AND COALESCE(DATE(ft.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) >= '{PARTITION_START}' --Valid record
)
-- Usage is defined as orgs with at least one asset qualification submitted
-- We use created_by user as it's the user who uploaded the qualification
SELECT
  fs.org_id,
  fs.uuid,
  DATE(fs.server_created_at) AS date,
  fs.created_by_polymorphic AS user_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_submissions_v0` fs
JOIN qtypes_eu q
  ON fs.form_template_uuid = q.uuid
LEFT OUTER JOIN users_eu u
  ON SPLIT_PART(fs.created_by_polymorphic, '-', 2) = u.user_id
  AND SPLIT_PART(fs.created_by_polymorphic, '-', 1) = 'user'
JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` o
  ON o.id = fs.org_id
WHERE
  fs.product_type = 8 --qualifications
  AND fs.server_created_at > '2025-01-01' -- Before quals was released
  AND fs.status = 1 -- 1 represents completed
  AND u.user_id IS NULL -- Exclude internal users
  AND COALESCE(DATE(fs.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) >= '{PARTITION_START}' --Valid record
  AND fs.created_by_polymorphic != 'user-1' -- Exclude system user records
  AND q.is_asset_type = TRUE -- Only asset qualifications

UNION ALL

SELECT
  fs.org_id,
  fs.uuid,
  DATE(fs.server_created_at) AS date,
  fs.created_by_polymorphic AS user_id
FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-forms-shard-1db/formsdb/form_submissions_v0` fs
JOIN qtypes_eu q
  ON fs.form_template_uuid = q.uuid
LEFT OUTER JOIN users_eu u
  ON SPLIT_PART(fs.created_by_polymorphic, '-', 2) = u.user_id
  AND SPLIT_PART(fs.created_by_polymorphic, '-', 1) = 'user'
JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` o
  ON o.id = fs.org_id
WHERE
  fs.product_type = 8 --qualifications
  AND fs.server_created_at > '2025-01-01' -- Before quals was released
  AND fs.status = 1 -- 1 represents completed
  AND u.user_id IS NULL -- Exclude internal users
  AND COALESCE(DATE(fs.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) >= '{PARTITION_START}' --Valid record
  AND fs.created_by_polymorphic != 'user-1' -- Exclude system user records
  AND q.is_asset_type = TRUE -- Only asset qualifications
