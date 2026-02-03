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
    `nvr_connection_state`: STRUCT<
      `nvr_id`: BIGINT,
      `nvr_serial`: STRING,
      `slot_info`: ARRAY<
        STRUCT<
          `slot_id`: INT,
          `connected_state`: BOOLEAN,
          `camera_name`: STRING,
          `camera_serial`: STRING,
          `camera_resolution`: STRING,
          `recording_state`: INT,
          `camera_id`: BIGINT,
          `track_id`: BIGINT
        >
      >,
      `unix_time_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
