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
    `trailer_air_line_status`: STRUCT<
      `temperature_celsius`: FLOAT,
      `pressure_kpa`: FLOAT,
      `sensor_diagnostics`: STRUCT<`battery_level_volts`: FLOAT>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
