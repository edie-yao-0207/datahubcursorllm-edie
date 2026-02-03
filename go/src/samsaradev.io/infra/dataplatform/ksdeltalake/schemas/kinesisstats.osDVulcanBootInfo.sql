`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `received_delta_seconds`: BIGINT,
  `is_start`: BOOLEAN,
  `is_end`: BOOLEAN,
  `is_databreak`: BOOLEAN,
  `int_value`: BIGINT,
  `double_value`: DOUBLE,
  `proto_value`: STRUCT<
    `vulcan_boot_info`: STRUCT<
      `boot_reason`: INT,
      `boot_count`: BIGINT,
      `build_string`: STRING,
      `filesystem_erase`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
