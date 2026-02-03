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
    `marathon_movement`: STRUCT<
      `motion_event`: STRUCT<`type`: INT>,
      `debug`: ARRAY<
        STRUCT<
          `variance_g`: FLOAT,
          `mean_g`: FLOAT,
          `max_g`: FLOAT,
          `min_g`: FLOAT,
          `timestamp_ms`: BIGINT,
          `data`: ARRAY<BIGINT>
        >
      >,
      `start_ms`: BIGINT,
      `duration_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
