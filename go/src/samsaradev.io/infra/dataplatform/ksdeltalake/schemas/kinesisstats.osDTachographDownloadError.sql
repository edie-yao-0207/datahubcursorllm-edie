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
    `tachograph_download_error`: STRUCT<
      `error_text`: STRING,
      `can_interface`: STRING,
      `error_code`: INT,
      `download_stage`: INT,
      `sent_sid`: BIGINT,
      `received_sid`: BIGINT,
      `response_code`: BIGINT,
      `download_trigger`: INT,
      `download_result`: INT,
      `smart_card_gateway_id`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
