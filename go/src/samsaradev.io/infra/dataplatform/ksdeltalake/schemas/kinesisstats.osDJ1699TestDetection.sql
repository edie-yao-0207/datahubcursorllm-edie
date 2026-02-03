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
    `j1699_test_detection`: STRUCT<
      `j1699_detection_reason`: INT,
      `had_complete_fix`: BOOLEAN,
      `latitude_nd`: BIGINT,
      `longitude_nd`: BIGINT,
      `time_to_detection_ms`: DECIMAL(20, 0),
      `region_check_enabled`: BOOLEAN,
      `bounding_box_min_latitude_nd`: BIGINT,
      `bounding_box_max_latitude_nd`: BIGINT,
      `bounding_box_min_longitude_nd`: BIGINT,
      `bounding_box_max_longitude_nd`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
