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
    `workforce_stream_channel_connectivity`: STRUCT<
      `connected`: BOOLEAN,
      `width`: BIGINT,
      `height`: BIGINT,
      `fps`: FLOAT,
      `start_time_ms`: BIGINT,
      `rfc6381_codec`: STRING,
      `bitrate`: DECIMAL(20, 0),
      `audio_availablility`: INT,
      `is_streaming_audio`: BOOLEAN,
      `audio_codec`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
