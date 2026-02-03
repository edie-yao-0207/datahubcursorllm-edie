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
`_synced_at` TIMESTAMP
