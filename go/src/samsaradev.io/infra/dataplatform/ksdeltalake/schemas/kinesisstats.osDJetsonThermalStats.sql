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
    `jetson_thermal_stats`: STRUCT<
      `bcpu_milli_degree_c`: BIGINT,
      `mcpu_milli_degree_c`: BIGINT,
      `gpu_milli_degree_c`: BIGINT,
      `pll_milli_degree_c`: BIGINT,
      `tboard_milli_degree_c`: BIGINT,
      `tdiode_milli_degree_c`: BIGINT,
      `pmic_milli_degree_c`: BIGINT,
      `thermal_fan_milli_degree_c`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
