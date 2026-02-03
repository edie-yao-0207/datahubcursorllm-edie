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
    `equipment_activity_input_debug`: STRUCT<
      `equipment_state`: INT,
      `equipment_last_update_age_ms`: DECIMAL(20, 0),
      `obd_engine_state`: INT,
      `obd_engine_last_update_age_ms`: DECIMAL(20, 0),
      `aux_input_signal_enabled`: BOOLEAN,
      `aux_input_signal_state`: INT,
      `aux_input_signal_last_update_age_ms`: DECIMAL(20, 0),
      `voltage_fft_state`: INT,
      `voltage_fft_last_update_age_ms`: DECIMAL(20, 0),
      `movement_state`: INT,
      `movement_last_update_age_ms`: DECIMAL(20, 0),
      `vibration_state`: INT,
      `vibration_last_update_age_ms`: DECIMAL(20, 0),
      `always_on_enabled`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
