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
    `data_platform_internal_test_stat`: STRUCT<
      `int_input`: BIGINT,
      `basic_data_input`: STRUCT<
        `sint_input`: BIGINT,
        `uint_input`: DECIMAL(20, 0),
        `bool_input`: BOOLEAN,
        `string_input`: STRING
      >,
      `advanced_data_input`: STRUCT<`bytes_input`: BINARY>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
