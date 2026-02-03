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
    `dashcam_seatbelt_data`: STRUCT<
      `label`: INT,
      `warning_count`: BIGINT,
      `confidence`: FLOAT,
      `gateway_id`: BIGINT,
      `event_id`: BIGINT,
      `ml_run_tag`: STRUCT<`run_id`: BIGINT>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
