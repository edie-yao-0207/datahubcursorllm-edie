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
    `widget_connect_info`: STRUCT<
      `device_id`: BIGINT,
      `generation`: BIGINT,
      `sequence`: BIGINT,
      `ticks_base`: BIGINT,
      `ticks_now`: BIGINT,
      `version_info`: BIGINT,
      `sw_rev`: STRING,
      `monitor_id`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
