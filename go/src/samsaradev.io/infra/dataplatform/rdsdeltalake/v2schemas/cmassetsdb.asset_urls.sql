`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`asset_ms` BIGINT,
`s3_bucket` STRING,
`filename` STRING,
`created_at` TIMESTAMP,
`detail` STRUCT<
  `end_ms`: BIGINT,
  `camera_ids`: ARRAY<BIGINT>,
  `resolution`: BIGINT,
  `user_has_no_permissions`: BOOLEAN,
  `asset_url_creation_audit`: ARRAY<
    STRUCT<
      `created_at_ms`: BIGINT,
      `created_by_service`: STRING,
      `workflow_metadata`: STRUCT<
        `workflow_uuid`: STRING,
        `workflow_name`: STRING,
        `workflow_step_name`: STRING
      >
    >
  >
>,
`_raw_detail` STRING,
`url_type` SHORT,
`resolution` SHORT,
`bitrate` BIGINT,
`codec` SHORT,
`date` STRING
