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
    `mdm9607_tsens`: STRUCT<
      `tsens_tz_sensor0_milli_degree_c`: BIGINT,
      `tsens_tz_sensor1_milli_degree_c`: BIGINT,
      `tsens_tz_sensor2_milli_degree_c`: BIGINT,
      `tsens_tz_sensor3_milli_degree_c`: BIGINT,
      `tsens_tz_sensor4_milli_degree_c`: BIGINT,
      `pa_therm0_milli_degree_c`: BIGINT,
      `vg_mcu_sensor0_milli_degree_c`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
