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
    `can_bus_error_info`: STRUCT<
      `total_frame_count`: BIGINT,
      `can_bus_type`: BIGINT,
      `can_error_list`: ARRAY<
        STRUCT<
          `error_class`: INT,
          `raw_payload_length`: BIGINT,
          `error_frame_data`: BIGINT,
          `instance_count`: BIGINT
        >
      >,
      `bus_id`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
