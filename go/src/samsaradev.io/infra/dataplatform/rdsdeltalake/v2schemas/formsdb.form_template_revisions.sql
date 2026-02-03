`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`form_template_uuid` STRING,
`server_created_at` TIMESTAMP,
`created_by` BIGINT,
`server_deleted_at` TIMESTAMP,
`product_specific_data_proto` STRUCT<
  `product_type`: INT,
  `form_data`: STRUCT<
    `approval_config`: STRUCT<
      `type`: INT,
      `single_approval_config`: STRUCT<
        `allow_manual_approver_selection`: BOOLEAN,
        `requirements`: STRUCT<
          `role_uuids`: ARRAY<BINARY>
        >,
        `approver_selection_type`: INT
      >
    >
  >,
  `course_data`: STRUCT<
    `estimated_duration_seconds`: DECIMAL(20, 0),
    `locale_ids`: ARRAY<STRING>,
    `category_uuid`: BINARY,
    `behaviors`: ARRAY<
      STRUCT<
        `type`: INT,
        `value`: BIGINT
      >
    >,
    `feature_config`: STRING,
    `scoring_type`: INT,
    `passing_threshold`: BIGINT,
    `thumbnail_s3_key`: STRING,
    `thumbnail_upload_status`: INT,
    `course_skeleton_version`: STRING,
    `thumbnail_media_item_uuid`: BINARY,
    `is_short_form`: BOOLEAN
  >,
  `asset_maintenance_settings_data`: STRUCT<
    `use_default_labor_rate`: BOOLEAN,
    `default_labor_rate_cents`: BIGINT,
    `change_approval_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `default_approvers`: STRUCT<
        `primary_user_ids`: ARRAY<BIGINT>
      >
    >,
    `close_approval_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `default_approvers`: STRUCT<
        `primary_user_ids`: ARRAY<BIGINT>
      >
    >,
    `cost_approval_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `threshold_cents`: BIGINT,
      `default_approvers`: STRUCT<
        `primary_user_ids`: ARRAY<BIGINT>
      >
    >,
    `tax_settings`: STRUCT<
      `default_tax_type`: INT,
      `default_tax_rate_percent`: BIGINT,
      `apply_tax_to_labor`: BOOLEAN
    >
  >,
  `qualification_data`: STRUCT<
    `disable_worker_view`: BOOLEAN,
    `disable_worker_upload`: BOOLEAN,
    `enable_worker_upload_without_approval`: BOOLEAN,
    `mvr_type`: INT
  >
>,
`_raw_product_specific_data_proto` STRING,
`draft_published_at` TIMESTAMP,
`server_updated_at` TIMESTAMP,
`partition` STRING
