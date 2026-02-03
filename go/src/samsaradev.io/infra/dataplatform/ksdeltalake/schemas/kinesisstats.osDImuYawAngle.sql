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
    `imu_yaw_angle`: STRUCT<
      `success`: BOOLEAN,
      `failure_reason`: BIGINT,
      `yaw_angle_degrees`: FLOAT,
      `left_turn_sample_count`: BIGINT,
      `right_turn_sample_count`: BIGINT,
      `strong_accel_brake_sample_count`: BIGINT,
      `side_direction`: STRUCT<
        `x`: FLOAT,
        `y`: FLOAT,
        `z`: FLOAT
      >,
      `forward_direction`: STRUCT<
        `x`: FLOAT,
        `y`: FLOAT,
        `z`: FLOAT
      >,
      `left_vector`: STRUCT<
        `x`: FLOAT,
        `y`: FLOAT,
        `z`: FLOAT
      >,
      `right_vector`: STRUCT<
        `x`: FLOAT,
        `y`: FLOAT,
        `z`: FLOAT
      >,
      `side_forward_orthogonality`: FLOAT,
      `left_right_angle_degrees`: FLOAT,
      `diff_with_current_orientation_yaw_angle`: BIGINT,
      `drive_time_since_start_secs`: BIGINT,
      `calibration_started_at_unix_ms`: DECIMAL(20, 0),
      `harmonic4th_and_convergence`: STRUCT<
        `confidence`: FLOAT,
        `side_projection`: FLOAT,
        `convergence_attempts`: BIGINT,
        `convergence_stable_count`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
