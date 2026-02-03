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
    `supplementary_location`: STRUCT<
      `has_fix`: BOOLEAN,
      `latitude_nanodegrees`: BIGINT,
      `longitude_nanodegrees`: BIGINT,
      `altitude_millimeters`: INT,
      `accuracy_millimeters`: BIGINT,
      `speed_milliknots`: BIGINT,
      `heading_millidegrees`: BIGINT,
      `timestamp_utc_ms`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
