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
    `motion_aggregate`: STRUCT<
      `camera_device_uuid`: BINARY,
      `stream_uid`: BINARY,
      `start_ms`: BIGINT,
      `duration_ms`: BIGINT,
      `data`: STRUCT<
        `matrix`: ARRAY<BIGINT>,
        `max_value`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
