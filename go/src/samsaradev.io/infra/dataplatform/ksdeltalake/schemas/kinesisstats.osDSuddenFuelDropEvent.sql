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
    `sudden_fuel_drop_event`: STRUCT<
      `event_id`: STRING,
      `change_start_ms`: DECIMAL(20, 0),
      `change_end_ms`: DECIMAL(20, 0),
      `fuel_level_before_millipercent`: BIGINT,
      `fuel_level_after_millipercent`: BIGINT,
      `confidence_level_millipercent`: BIGINT,
      `analysis_window_before_start_ms`: DECIMAL(20, 0),
      `analysis_window_after_end_ms`: DECIMAL(20, 0),
      `video_clips`: ARRAY<
        STRUCT<
          `position`: INT,
          `start_ms`: DECIMAL(20, 0),
          `end_ms`: DECIMAL(20, 0)
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
