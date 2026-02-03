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
    `vanishing_point_state`: STRUCT<
      `calibration_state`: INT,
      `calibration_num_valid_samples`: BIGINT,
      `calibrated`: BOOLEAN,
      `vanishing_point`: STRUCT<
        `x_pct`: FLOAT,
        `y_pct`: FLOAT
      >,
      `calibration_version`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
