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
    `dashcam_driver_album`: STRUCT<
      `start_ms`: BIGINT,
      `end_ms`: BIGINT,
      `image_infos`: ARRAY<
        STRUCT<
          `x`: INT,
          `y`: INT,
          `width`: INT,
          `height`: INT
        >
      >,
      `ml_run_tag`: STRUCT<`run_id`: BIGINT>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
