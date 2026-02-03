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
    `command_scheduler_stats`: STRUCT<
      `command_stats`: ARRAY<
        STRUCT<
          `command`: STRUCT<
            `request_id`: BIGINT,
            `response_id`: BIGINT,
            `data_identifier`: BIGINT,
            `vehicle_bus`: INT,
            `protocol`: INT,
            `data_identifier_uint64`: DECIMAL(20, 0)
          >,
          `expiration_stat`: STRUCT<`expiration_offset_ms`: BIGINT>,
          `request_response_stat`: STRUCT<
            `request_count`: BIGINT,
            `total_response_count`: BIGINT,
            `any_response_count`: BIGINT
          >,
          `is_stopped`: BOOLEAN,
          `calculated_max_command_no_response_to_expire`: BIGINT,
          `read_period_ms`: BIGINT,
          `calculated_max_command_no_response_to_derate`: BIGINT,
          `derated_stat`: STRUCT<
            `overall_derate_instances`: BIGINT,
            `latest_derate_offset_ms`: BIGINT
          >,
          `excluded_from_derating_by_config`: BOOLEAN
        >
      >,
      `window_duration_ms`: DECIMAL(20, 0),
      `bus_id`: INT,
      `metadata`: STRUCT<
        `configured_max_command_no_response_count_floor`: BIGINT,
        `configured_max_command_no_response_count_ceiling`: BIGINT,
        `command_expiration_enabled`: BOOLEAN,
        `enqueue_jitter_enabled`: BOOLEAN,
        `enqueue_jitter_ms`: BIGINT,
        `minimum_jitter_period_multiple`: BIGINT,
        `command_derate_enabled`: BOOLEAN,
        `derated_command_interval_ms`: BIGINT,
        `configured_window_interval_ms`: BIGINT
      >,
      `session_id`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
