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
    `engine_state`: STRUCT<
      `offset_ms`: BIGINT,
      `offset_type`: INT,
      `has_read_movement_this_session`: BOOLEAN,
      `movement_requirement_for_idle_enabled`: BOOLEAN,
      `has_valid_cached_movement`: BOOLEAN,
      `engine_activity_internal_feature_used`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
