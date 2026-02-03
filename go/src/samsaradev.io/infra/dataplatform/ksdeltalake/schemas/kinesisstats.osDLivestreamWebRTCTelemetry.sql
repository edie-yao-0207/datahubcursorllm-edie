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
    `livestream_webrtc_telemetry`: STRUCT<
      `local_candidate_type`: INT,
      `remote_candidate_type`: INT,
      `connection_succeeded`: BOOLEAN,
      `connection_establishment_ms`: BIGINT,
      `connection_duration_ms`: BIGINT,
      `user_id`: BIGINT,
      `end_reason`: INT,
      `download_bytes`: BIGINT,
      `upload_bytes`: BIGINT,
      `connection_never_used`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
