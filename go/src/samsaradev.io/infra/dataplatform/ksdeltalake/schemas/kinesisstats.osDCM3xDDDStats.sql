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
    `cm3x_vision_stats`: STRUCT<
      `fps`: FLOAT,
      `latency_ms`: FLOAT,
      `frames_total`: BIGINT,
      `frames_total_with_face`: BIGINT,
      `frames_while_moving`: BIGINT,
      `frames_while_moving_with_face`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
