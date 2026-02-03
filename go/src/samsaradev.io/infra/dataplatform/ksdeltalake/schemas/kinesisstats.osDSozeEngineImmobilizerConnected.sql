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
    `soze_engine_immobilizer_connected`: STRUCT<
      `connected`: INT,
      `firmware_version`: DECIMAL(20, 0),
      `serial`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
