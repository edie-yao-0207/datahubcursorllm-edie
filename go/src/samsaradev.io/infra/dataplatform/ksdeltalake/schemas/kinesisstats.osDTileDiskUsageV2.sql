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
    `tile_disk_usage`: STRUCT<
      `tile_count`: DECIMAL(20, 0),
      `tile_bytes`: DECIMAL(20, 0),
      `session_stats`: STRUCT<
        `session_nonce`: BIGINT,
        `tile_lookups`: DECIMAL(20, 0),
        `disk_cache_hits`: DECIMAL(20, 0),
        `download_attempts`: DECIMAL(20, 0),
        `download_success_count`: DECIMAL(20, 0),
        `override_disk_cache_hits`: DECIMAL(20, 0),
        `override_download_attempts`: DECIMAL(20, 0),
        `override_download_success_count`: DECIMAL(20, 0),
        `tile_download_etag_hits`: DECIMAL(20, 0),
        `override_download_etag_hits`: DECIMAL(20, 0),
        `session_start_time_utc_ms`: DECIMAL(20, 0),
        `tile_version`: STRING,
        `tiles_cache_key`: STRING,
        `overrides_cache_key`: STRING,
        `bridge_location_lookups`: DECIMAL(20, 0),
        `bridge_location_disk_cache_hits`: DECIMAL(20, 0),
        `bridge_location_download_attempts`: DECIMAL(20, 0),
        `bridge_location_download_success_count`: DECIMAL(20, 0),
        `bridge_location_download_etag_hits`: DECIMAL(20, 0),
        `bridge_location_version`: STRING,
        `bridge_location_cache_key`: STRING
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
