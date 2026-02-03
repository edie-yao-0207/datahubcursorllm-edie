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
    `anomaly_event`: STRUCT<
      `boot_count`: INT,
      `build`: STRING,
      `description`: STRING,
      `device_id`: BIGINT,
      `epoch_time_ms`: BIGINT,
      `service_name`: STRING,
      `manager_name`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
