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
    `marathon_raw_can_debug`: STRUCT<
      `total_log_event_duration_ms`: BIGINT,
      `log_reason`: INT,
      `baud_rate`: INT,
      `can_log`: ARRAY<
        STRUCT<
          `protocol_type`: INT,
          `log_start_to_message_delta_ms`: BIGINT,
          `is_tx`: BOOLEAN,
          `msg_id`: BIGINT,
          `data_bytes`: BINARY,
          `num_times_message_observed`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
