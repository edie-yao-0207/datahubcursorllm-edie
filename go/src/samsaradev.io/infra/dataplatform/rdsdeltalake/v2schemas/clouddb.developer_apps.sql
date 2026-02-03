`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`uuid` STRING,
`developer_id` STRING,
`name` STRING,
`description` STRING,
`app_metadata` STRUCT<
  `app_secret`: BINARY,
  `app_logo_url`: STRING,
  `redirect_uris`: ARRAY<STRING>,
  `required_scopes`: ARRAY<STRING>,
  `direct_install_url`: STRING,
  `app_secret_hash`: BINARY,
  `data_sharing_agreement_type`: INT,
  `authz_role`: STRUCT<
    `permission_sets`: ARRAY<
      STRUCT<
        `id`: STRING,
        `view`: BOOLEAN,
        `edit`: BOOLEAN,
        `create`: BOOLEAN,
        `update`: BOOLEAN,
        `delete`: BOOLEAN,
        `other_action_sets`: ARRAY<
          STRUCT<
            `id`: STRING,
            `edit`: BOOLEAN
          >
        >
      >
    >,
    `permissions`: ARRAY<
      STRUCT<
        `id`: STRING,
        `view`: BOOLEAN,
        `edit`: BOOLEAN,
        `create`: BOOLEAN,
        `update`: BOOLEAN,
        `delete`: BOOLEAN,
        `other_action_sets`: ARRAY<
          STRUCT<
            `id`: STRING,
            `edit`: BOOLEAN
          >
        >
      >
    >
  >,
  `is_exempt_from_install_limit`: BOOLEAN,
  `publish_app_page`: BOOLEAN,
  `slug`: STRING,
  `github_url`: STRING,
  `function_name`: STRING,
  `slack_reviewers`: STRING
>,
`_raw_app_metadata` STRING,
`approval_state` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`created_by` BIGINT,
`deactivated` BYTE,
`app_type` BIGINT,
`beta_code` STRING,
`version` INT,
`version_timestamp` TIMESTAMP,
`partition` STRING
