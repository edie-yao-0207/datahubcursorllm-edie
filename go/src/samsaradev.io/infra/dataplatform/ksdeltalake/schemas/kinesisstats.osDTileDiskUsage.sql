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
      `tile_count`: BIGINT,
      `tile_bytes`: BIGINT,
      `session_stats`: STRUCT<
        `session_nonce`: BIGINT,
        `tile_lookups`: BIGINT,
        `disk_cache_hits`: BIGINT,
        `download_attempts`: BIGINT,
        `download_success_count`: BIGINT,
        `override_disk_cache_hits`: BIGINT,
        `override_download_attempts`: BIGINT,
        `override_download_success_count`: BIGINT,
        `tile_download_etag_hits`: BIGINT,
        `override_download_etag_hits`: BIGINT,
        `session_start_time_utc_ms`: BIGINT,
        `tile_version`: STRING,
        `tiles_cache_key`: STRING,
        `overrides_cache_key`: STRING,
        `bridge_location_lookups`: BIGINT,
        `bridge_location_disk_cache_hits`: BIGINT,
        `bridge_location_download_attempts`: BIGINT,
        `bridge_location_download_success_count`: BIGINT,
        `bridge_location_download_etag_hits`: BIGINT,
        `bridge_location_version`: STRING,
        `bridge_location_cache_key`: STRING
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
