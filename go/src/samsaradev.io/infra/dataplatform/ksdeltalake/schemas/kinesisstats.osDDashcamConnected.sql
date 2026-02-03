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
    `dashcam_connection_state`: STRUCT<
      `camera_id`: BIGINT,
      `connected_state`: BOOLEAN,
      `camera_serial`: STRING,
      `camera_product_type`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
