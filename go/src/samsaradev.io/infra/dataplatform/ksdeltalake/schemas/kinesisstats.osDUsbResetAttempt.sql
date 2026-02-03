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
    `usb_reset_attempt`: STRUCT<
      `successful`: BOOLEAN,
      `time_since_reset_ms`: BIGINT,
      `config`: STRUCT<
        `enabled`: INT,
        `interval_between_resets_ms`: BIGINT,
        `reset_duration_ms`: BIGINT,
        `max_resets`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
