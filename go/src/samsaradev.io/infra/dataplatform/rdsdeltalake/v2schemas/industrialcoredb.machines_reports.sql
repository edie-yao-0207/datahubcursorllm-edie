`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`name` STRING,
`config` STRUCT<
  `machine_ids`: ARRAY<BIGINT>,
  `column_metadata`: ARRAY<
    STRUCT<
      `type_id`: BIGINT,
      `summary_function`: INT,
      `column_type`: INT,
      `machine_metadata`: STRUCT<
        `is_custom`: BOOLEAN,
        `key`: STRING
      >
    >
  >,
  `migrated`: BOOLEAN
>,
`_raw_config` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted` BYTE,
`partition` STRING
