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
    `voltage_ignition_detection`: STRUCT<
      `metadata`: STRUCT<
        `algorithm_version`: BIGINT,
        `sample_period_ms`: BIGINT,
        `voltage_jump_detector_config`: STRUCT<
          `polling_interval_ms`: BIGINT,
          `num_samples`: INT,
          `threshold_mv`: BIGINT,
          `min_mv`: BIGINT,
          `max_mv`: BIGINT,
          `enable_for_non_passenger_vehicles`: BOOLEAN,
          `use_min_voltage_in_window`: INT,
          `num_samples_config_enabled`: INT,
          `threshold_mv_config_enabled`: INT,
          `min_mv_config_enabled`: INT,
          `max_mv_config_enabled`: INT,
          `non_passenger_vehicles_enabled`: INT,
          `voltage_jump_detector_enabled`: INT
        >,
        `voltage_jump_detector_num_samples`: BIGINT,
        `voltage_jump_detector_threshold_mv`: BIGINT,
        `voltage_jump_detector_min_mv`: BIGINT,
        `voltage_jump_detector_max_mv`: BIGINT,
        `voltage_jump_detector_non_passenger_vehicle_detection_enabled`: BOOLEAN,
        `voltage_jump_detector_use_min_voltage_in_window`: BOOLEAN,
        `voltage_jump_detector_max_voltage_jump_count_between_trips_enabled`: BOOLEAN,
        `voltage_jump_detector_max_voltage_jump_count_between_trips`: BIGINT
      >,
      `detection_offsets_ms`: ARRAY<BIGINT>,
      `detection_min_observed_voltages_mv`: ARRAY<BIGINT>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
