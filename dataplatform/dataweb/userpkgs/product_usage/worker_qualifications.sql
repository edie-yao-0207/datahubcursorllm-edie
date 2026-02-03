WITH qtypes_us AS (
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
  FROM formsdb_shards.form_templates ft
  LEFT JOIN formsdb_shards.form_template_revisions ftr
    ON ftr.uuid = ft.current_template_revision_uuid
  LEFT JOIN formsdb_shards.form_template_revision_contents ftrc
    ON ftr.uuid = ftrc.form_template_revision_uuid
  WHERE
    ft.product_type = 8
    AND COALESCE(DATE(ft.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) >= '{PARTITION_START}' --Valid record
)
-- Usage is defined as orgs with at least one worker qualification submitted
-- We use created_by user as it's the user who uploaded the qualification
SELECT
  fs.org_id,
  fs.uuid,
  DATE(fs.server_created_at) AS date,
  fs.created_by_polymorphic AS user_id
FROM formsdb_shards.form_submissions fs
JOIN qtypes_us q
    ON fs.form_template_uuid = q.uuid
LEFT OUTER JOIN users_us u
  ON SPLIT_PART(fs.created_by_polymorphic, '-', 2) = u.user_id
  AND SPLIT_PART(fs.created_by_polymorphic, '-', 1) = 'user'
JOIN clouddb.organizations o
  ON o.id = fs.org_id
WHERE
  fs.product_type = 8 --qualifications
  AND fs.server_created_at > '2025-01-01' -- Before quals was released
  AND fs.status = 1 -- 1 represents completed
  AND u.user_id IS NULL -- Exclude internal users
  AND COALESCE(DATE(fs.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) >= '{PARTITION_START}' --Valid record
  AND fs.created_by_polymorphic != 'user-1' -- Exclude system user records
  AND q.is_worker_type = TRUE -- Only worker qualifications
