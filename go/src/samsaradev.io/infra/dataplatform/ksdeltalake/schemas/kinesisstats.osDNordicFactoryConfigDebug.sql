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
    `nordic_factory_config_debug`: STRUCT<
      `hw_rev`: INT,
      `accel_x_ofs_ug`: INT,
      `accel_y_ofs_ug`: INT,
      `accel_z_ofs_ug`: INT,
      `dfu_enabled`: BOOLEAN,
      `uart_enabled`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
