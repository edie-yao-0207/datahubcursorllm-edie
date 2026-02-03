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
    `ble_peripheral_accel_data`: STRUCT<
      `x_ug`: ARRAY<INT>,
      `y_ug`: ARRAY<INT>,
      `z_ug`: ARRAY<INT>,
      `num_samples`: BIGINT,
      `delta_encoded`: BOOLEAN,
      `in_motion`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
