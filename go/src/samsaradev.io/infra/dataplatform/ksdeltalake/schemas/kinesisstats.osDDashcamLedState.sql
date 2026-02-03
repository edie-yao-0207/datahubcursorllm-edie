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
    `dashcam_led_state`: STRUCT<
      `enabled`: BOOLEAN,
      `rgb_color`: STRUCT<
        `red`: BIGINT,
        `green`: BIGINT,
        `blue`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
