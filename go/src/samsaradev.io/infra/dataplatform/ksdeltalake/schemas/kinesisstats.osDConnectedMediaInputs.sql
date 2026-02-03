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
    `connected_media_inputs`: STRUCT<
      `primary`: STRUCT<
        `video_capability`: STRUCT<
          `resolution`: INT,
          `framerate`: BIGINT
        >,
        `audio_capability`: STRUCT<`samplerate`: BIGINT>
      >,
      `secondary`: STRUCT<
        `video_capability`: STRUCT<
          `resolution`: INT,
          `framerate`: BIGINT
        >,
        `audio_capability`: STRUCT<`samplerate`: BIGINT>
      >,
      `analog_1`: STRUCT<
        `video_capability`: STRUCT<
          `resolution`: INT,
          `framerate`: BIGINT
        >,
        `audio_capability`: STRUCT<`samplerate`: BIGINT>
      >,
      `analog_2`: STRUCT<
        `video_capability`: STRUCT<
          `resolution`: INT,
          `framerate`: BIGINT
        >,
        `audio_capability`: STRUCT<`samplerate`: BIGINT>
      >,
      `analog_3`: STRUCT<
        `video_capability`: STRUCT<
          `resolution`: INT,
          `framerate`: BIGINT
        >,
        `audio_capability`: STRUCT<`samplerate`: BIGINT>
      >,
      `analog_4`: STRUCT<
        `video_capability`: STRUCT<
          `resolution`: INT,
          `framerate`: BIGINT
        >,
        `audio_capability`: STRUCT<`samplerate`: BIGINT>
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
