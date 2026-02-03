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
    `obd_segmented_event`: STRUCT<
      `duration_ms`: BIGINT,
      `triggered_conditions`: ARRAY<
        STRUCT<
          `obd_value`: INT,
          `mean_value`: BIGINT,
          `min_value`: BIGINT,
          `max_value`: BIGINT,
          `threshold_value`: BIGINT,
          `operator`: INT,
          `condition_met_duration_ms`: BIGINT,
          `value_count`: DECIMAL(20, 0)
        >
      >,
      `min_active_duration_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
