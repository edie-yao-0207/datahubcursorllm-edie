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
    `media_storage_info`: STRUCT<
      `video_storage_used_bytes`: DECIMAL(20, 0),
      `stills_storage_used_bytes`: DECIMAL(20, 0),
      `total_used_bytes`: DECIMAL(20, 0),
      `available_disk_space_bytes`: DECIMAL(20, 0),
      `media_available_disk_space_bytes`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
