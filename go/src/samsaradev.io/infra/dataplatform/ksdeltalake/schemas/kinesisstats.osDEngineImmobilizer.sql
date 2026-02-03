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
    `engine_immobilizer_status`: STRUCT<
      `state`: INT,
      `configured_state`: INT,
      `usb_relay_controller_connected`: BOOLEAN,
      `engine_running`: BOOLEAN,
      `immobilizer_type`: INT,
      `safety_information`: STRUCT<
        `safe_to_immobilize`: INT,
        `ecu_speed_kmph`: DECIMAL(20, 0),
        `safety_threshold_kmph`: DECIMAL(20, 0)
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
