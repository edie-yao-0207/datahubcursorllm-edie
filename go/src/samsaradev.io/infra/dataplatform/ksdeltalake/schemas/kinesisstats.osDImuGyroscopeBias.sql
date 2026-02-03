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
    `imu_gyroscope_bias`: STRUCT<
      `bias_dps`: STRUCT<
        `x`: FLOAT,
        `y`: FLOAT,
        `z`: FLOAT
      >,
      `has_valid_temperature`: BOOLEAN,
      `temperature_c`: FLOAT,
      `drive_time_since_start_secs`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
