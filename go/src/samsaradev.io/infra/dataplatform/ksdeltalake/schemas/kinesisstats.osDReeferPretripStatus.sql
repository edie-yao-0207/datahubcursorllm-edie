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
    `reefer_pretrip_status`: STRUCT<
      `carrier`: STRUCT<
        `overall_status`: INT,
        `current_test_number`: BIGINT,
        `current_test_time_elapsed_ms`: BIGINT,
        `tests`: ARRAY<
          STRUCT<
            `test_number`: BIGINT,
            `status`: INT
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
