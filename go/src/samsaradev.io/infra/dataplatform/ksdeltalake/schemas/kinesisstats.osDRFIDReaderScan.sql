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
    `rfid_scan_event`: STRUCT<
      `event_id`: BIGINT,
      `card_number`: BIGINT,
      `card_string`: STRING,
      `invalid_scan`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
