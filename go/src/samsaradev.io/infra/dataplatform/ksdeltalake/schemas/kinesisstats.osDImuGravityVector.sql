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
    `imu_gravity_vector`: STRUCT<
      `gravity_vector_g`: STRUCT<
        `x`: FLOAT,
        `y`: FLOAT,
        `z`: FLOAT
      >,
      `sample_count`: BIGINT,
      `angle_with_current_orientation`: FLOAT,
      `drive_time_since_start_secs`: BIGINT,
      `calibration_started_at_unix_ms`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
