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
    `remote_sensing_measurement`: STRUCT<
      `distance_mm`: BIGINT,
      `signal_to_noise_ratio`: BIGINT,
      `status`: INT,
      `bifrost_app_error_code`: BIGINT,
      `fw_version`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
