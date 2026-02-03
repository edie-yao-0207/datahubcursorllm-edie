`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`name` STRING,
`permissions` STRUCT<
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
`_raw_permissions` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`name_identifier` STRING,
`partition` STRING
