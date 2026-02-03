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
    `camera_event`: STRUCT<
      `recall_id`: BIGINT,
      `unix_event_time_ms`: BIGINT,
      `asset_type`: INT,
      `trigger_reason`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
