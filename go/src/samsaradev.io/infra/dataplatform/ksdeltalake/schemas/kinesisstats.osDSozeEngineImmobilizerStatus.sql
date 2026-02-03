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
    `soze_engine_immobilizer_status`: STRUCT<
      `immobilizer_connected`: INT,
      `tamper_state`: INT,
      `relay_1`: STRUCT<
        `read_state`: INT,
        `desired_state_without_safety`: INT,
        `state_change_reason`: INT,
        `set_state`: INT,
        `last_open_nonce`: BIGINT,
        `request_uuid`: BINARY
      >,
      `relay_2`: STRUCT<
        `read_state`: INT,
        `desired_state_without_safety`: INT,
        `state_change_reason`: INT,
        `set_state`: INT,
        `last_open_nonce`: BIGINT,
        `request_uuid`: BINARY
      >,
      `safety_information`: STRUCT<
        `engine_running`: INT,
        `gps_speed_kmph`: DECIMAL(20, 0),
        `ecu_speed_kmph`: DECIMAL(20, 0),
        `safety_threshold_kmph`: DECIMAL(20, 0),
        `safe_to_immobilize`: INT
      >,
      `relay_1_safety_information`: STRUCT<
        `engine_running`: INT,
        `gps_speed_kmph`: DECIMAL(20, 0),
        `ecu_speed_kmph`: DECIMAL(20, 0),
        `safety_threshold_kmph`: DECIMAL(20, 0),
        `safe_to_immobilize`: INT
      >,
      `relay_2_safety_information`: STRUCT<
        `engine_running`: INT,
        `gps_speed_kmph`: DECIMAL(20, 0),
        `ecu_speed_kmph`: DECIMAL(20, 0),
        `safety_threshold_kmph`: DECIMAL(20, 0),
        `safe_to_immobilize`: INT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
