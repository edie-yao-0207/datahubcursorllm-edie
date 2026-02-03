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
    `imu_short_term_gravity`: STRUCT<
      `short_term_gravity_g`: STRUCT<
        `x`: FLOAT,
        `y`: FLOAT,
        `z`: FLOAT
      >,
      `sample_count`: BIGINT,
      `angle_with_previous_vector_degrees`: FLOAT,
      `drive_time_since_start_secs`: BIGINT,
      `calibration_started_at_unix_ms`: DECIMAL(20, 0),
      `standard_deviation_g`: STRUCT<
        `x`: FLOAT,
        `y`: FLOAT,
        `z`: FLOAT
      >,
      `angle_with_current_orientation_gravity_vector`: FLOAT,
      `has_valid_temperature`: BOOLEAN,
      `temperature_c`: FLOAT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
