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
    `weather_alert_status`: STRUCT<
      `weather_type`: INT,
      `precipitation_type`: INT,
      `weather_severity`: INT,
      `event_type`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
