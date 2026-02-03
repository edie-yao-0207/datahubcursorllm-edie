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
    `pressure_vessel_health`: STRUCT<
      `temperature_milli_celsius`: INT,
      `pressure_psi`: FLOAT,
      `sensor_diagnostics`: STRUCT<`battery_level_percentage`: BIGINT>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
