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
    `carb_failure_report`: STRUCT<
      `failure`: INT,
      `session_message_id`: DECIMAL(20, 0),
      `current_attempt_count`: BIGINT,
      `max_attempts_allowed_count`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
