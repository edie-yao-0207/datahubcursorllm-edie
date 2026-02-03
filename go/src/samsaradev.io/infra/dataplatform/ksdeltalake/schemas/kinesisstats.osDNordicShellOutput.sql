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
    `nordic_shell_output`: STRUCT<
      `shell_messages`: ARRAY<
        STRUCT<
          `source`: INT,
          `contents`: STRING,
          `binary_contents`: BINARY,
          `cli_command_apply`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
