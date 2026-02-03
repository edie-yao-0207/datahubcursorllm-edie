`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `video_tokens_data`: STRUCT<
    `streams`: ARRAY<
      STRUCT<
        `media_input`: INT,
        `embeddings`: ARRAY<
          STRUCT<
            `offset_ms`: INT,
            `data`: BINARY
          >
        >
      >
    >
  >
>,
`_synced_at` TIMESTAMP,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `is_start`: BOOLEAN,
  `is_end`: BOOLEAN,
  `is_databreak`: BOOLEAN,
  `int_value`: BIGINT,
  `double_value`: DOUBLE,
  `proto_value`: STRUCT<
    `video_tokens`: STRUCT<
      `model_version`: STRING,
      `media_inputs`: ARRAY<INT>
    >
  >,
  `received_delta_seconds`: BIGINT
>
