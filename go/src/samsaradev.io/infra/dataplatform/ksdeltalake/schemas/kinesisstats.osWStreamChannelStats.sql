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
    `workforce_stream_channel_stats`: STRUCT<
      `video_duration_ms`: BIGINT,
      `video_bytes`: DECIMAL(20, 0),
      `video_frame_count`: DECIMAL(20, 0),
      `video_keyframe_count`: DECIMAL(20, 0),
      `video_sps_change`: BOOLEAN,
      `video_sps_width_px`: BIGINT,
      `video_sps_height_px`: BIGINT,
      `video_sps_fps`: BIGINT,
      `uncompressed_audio_bytes`: DECIMAL(20, 0),
      `audio_samples_count`: DECIMAL(20, 0),
      `audio_sample_rate`: DECIMAL(20, 0),
      `compressed_audio_bytes`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
