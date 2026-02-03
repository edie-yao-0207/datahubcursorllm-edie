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
    `rolling_stop_algorithm_filter_reason`: STRUCT<
      `filter_reason`: INT,
      `stop_location`: STRUCT<
        `location_type`: INT,
        `node_id`: DECIMAL(20, 0),
        `layer_source`: INT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
