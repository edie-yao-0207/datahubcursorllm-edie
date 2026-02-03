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
    `incremental_cellular_usage`: STRUCT<
      `bucket`: ARRAY<
        STRUCT<
          `bucket_type`: INT,
          `upload_bytes`: BIGINT,
          `download_bytes`: BIGINT
        >
      >,
      `total_upload_bytes`: BIGINT,
      `total_download_bytes`: BIGINT,
      `attached_device_usage`: ARRAY<
        STRUCT<
          `object_id`: BIGINT,
          `bucket`: ARRAY<
            STRUCT<
              `bucket_type`: INT,
              `upload_bytes`: BIGINT,
              `download_bytes`: BIGINT
            >
          >,
          `total_upload_bytes`: BIGINT,
          `total_download_bytes`: BIGINT,
          `hub_client_usage`: ARRAY<
            STRUCT<
              `usage_type`: INT,
              `upload_bytes`: BIGINT,
              `download_bytes`: BIGINT
            >
          >
        >
      >,
      `hub_client_usage`: ARRAY<
        STRUCT<
          `usage_type`: INT,
          `upload_bytes`: BIGINT,
          `download_bytes`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
