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
    `wake_up_sms_sent`: STRUCT<
      `device_iccid`: STRING,
      `source_id`: STRING,
      `priority`: INT,
      `validity_period_ms`: BIGINT,
      `replace_if_present`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
