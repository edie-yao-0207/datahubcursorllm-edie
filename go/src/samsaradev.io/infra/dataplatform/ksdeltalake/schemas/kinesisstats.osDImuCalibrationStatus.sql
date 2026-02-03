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
    `imu_calibration_status`: STRUCT<
      `gyroscope_bias`: STRUCT<`algo_state`: INT>,
      `short_term_gravity`: STRUCT<
        `algo_state`: INT,
        `sample_count`: BIGINT,
        `calibration_started_at_unixtime_ms`: DECIMAL(20, 0)
      >,
      `gravity`: STRUCT<
        `algo_state`: INT,
        `sample_count`: BIGINT,
        `calibration_started_at_unixtime_ms`: DECIMAL(20, 0)
      >,
      `yaw_angle`: STRUCT<
        `algo_state`: INT,
        `left_turn_sample_count`: BIGINT,
        `right_turn_sample_count`: BIGINT,
        `strong_accel_brake_sample_count`: BIGINT,
        `calibration_started_at_unixtime_ms`: DECIMAL(20, 0),
        `convergence_stable_count`: BIGINT,
        `convergence_attempts`: BIGINT,
        `latest_estimate_degrees`: INT,
        `latest_side_projection`: FLOAT,
        `latest_confidence`: FLOAT
      >,
      `current_calibration_index`: BIGINT,
      `drive_time_since_start_secs`: BIGINT,
      `config`: STRUCT<
        `calibration_reset`: STRUCT<
          `sent_at_unix_ms`: DECIMAL(20, 0),
          `reset_tag`: BIGINT
        >,
        `sample_collection`: STRUCT<
          `strong_turn_min_gyro_norm_dps`: INT,
          `no_turn_max_gyro_norm_dps`: INT,
          `save_progress_interval_secs`: BIGINT
        >,
        `gyroscope_bias`: STRUCT<
          `gyro_at_rest_max_norm_dps`: BIGINT,
          `gyro_at_rest_max_gyro_slope_filter_dps`: FLOAT,
          `gyro_at_rest_max_accel_slope_filter_g`: FLOAT,
          `gyro_bias_max_valid_value_dps`: BIGINT,
          `gyro_bias_update_min_change_dps`: FLOAT
        >,
        `short_term_gravity`: STRUCT<
          `accel_samples_max_offset_from_1g`: FLOAT,
          `lock_sample_count`: BIGINT,
          `lock_max_std_dev_g`: FLOAT,
          `orientation_change_threshold_degrees`: BIGINT
        >,
        `gravity`: STRUCT<
          `accel_samples_max_offset_from_1g`: FLOAT,
          `lock_sample_count`: BIGINT
        >,
        `yaw_angle`: STRUCT<
          `left_right_turn_gyro_z_axis_threshold_dps`: INT,
          `left_right_turn_min_accel_mag_g`: FLOAT,
          `strong_accel_brake_min_accel_mag_g`: FLOAT,
          `strong_accel_brake_max_z_axis_g`: FLOAT,
          `lock_min_left_and_right_sample_count`: BIGINT,
          `lock_min_strong_accel_or_brake_sample_count`: BIGINT,
          `lock_validity_max_forward_side_dot_product`: FLOAT,
          `lock_validity_min_left_right_angle_degrees`: BIGINT
        >,
        `orientation_updates`: STRUCT<
          `min_gravity_change_degrees`: BIGINT,
          `min_yaw_angle_change_degrees`: BIGINT
        >
      >,
      `algorithm_version`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
