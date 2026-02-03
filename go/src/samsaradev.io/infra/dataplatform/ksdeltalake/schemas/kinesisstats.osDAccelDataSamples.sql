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
    `accel_data_samples`: STRUCT<
      `samples`: ARRAY<
        STRUCT<
          `x_ug`: INT,
          `y_ug`: INT,
          `z_ug`: INT
        >
      >,
      `sample_rate_mhz`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
