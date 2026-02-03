`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`uuid` STRING,
`org_id` BIGINT,
`policy_uuid` STRING,
`type` INT,
`effective_from` TIMESTAMP,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted_at` TIMESTAMP,
`config` STRUCT<
  `public_holiday`: STRUCT<
    `days`: ARRAY<
      STRUCT<
        `start_of_day_ms`: BIGINT,
        `name`: STRING,
        `paid`: BOOLEAN
      >
    >
  >
>,
`_raw_config` STRING,
`partition` STRING
