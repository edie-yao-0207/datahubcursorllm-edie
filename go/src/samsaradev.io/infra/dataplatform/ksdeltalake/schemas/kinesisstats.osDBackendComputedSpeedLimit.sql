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
    `backend_computed_speed_limit`: STRUCT<
      `latitude_degrees`: DOUBLE,
      `longitude_degrees`: DOUBLE,
      `has_way_id`: BOOLEAN,
      `way_id`: DECIMAL(20, 0),
      `has_speed_limit`: BOOLEAN,
      `speed_limit_meters_per_second`: DOUBLE,
      `map_matching_method`: INT,
      `speed_limit_source`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
