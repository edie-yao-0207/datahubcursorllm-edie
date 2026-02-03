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
    `nordic_reefer_debug`: STRUCT<
      `thermoking`: STRUCT<
        `status`: BIGINT,
        `request_bytes`: BINARY,
        `response_bytes`: BINARY
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
