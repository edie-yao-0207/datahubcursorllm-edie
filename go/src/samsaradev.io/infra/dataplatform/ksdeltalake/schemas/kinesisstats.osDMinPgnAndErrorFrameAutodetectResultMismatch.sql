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
    `min_pgn_and_error_frame_autodetect_results`: STRUCT<
      `min_pgn_bitrate_result`: BIGINT,
      `error_frame_bitrate_result`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
