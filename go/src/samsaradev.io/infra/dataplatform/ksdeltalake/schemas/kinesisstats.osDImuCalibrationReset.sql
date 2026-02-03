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
    `imu_calibration_reset`: STRUCT<
      `reset_reason`: INT,
      `drive_time_since_start_secs`: BIGINT,
      `short_term_gravity_change_reset`: STRUCT<
        `current_short_term_gravity`: STRUCT<
          `x`: FLOAT,
          `y`: FLOAT,
          `z`: FLOAT
        >,
        `previous_short_term_gravity`: STRUCT<
          `x`: FLOAT,
          `y`: FLOAT,
          `z`: FLOAT
        >,
        `angle_degrees`: FLOAT,
        `threshold_degrees`: BIGINT
      >,
      `short_long_term_gravity_diff_reset`: STRUCT<
        `short_term_gravity`: STRUCT<
          `x`: FLOAT,
          `y`: FLOAT,
          `z`: FLOAT
        >,
        `long_term_gravity`: STRUCT<
          `x`: FLOAT,
          `y`: FLOAT,
          `z`: FLOAT
        >,
        `angle_degrees`: FLOAT,
        `threshold_degrees`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
