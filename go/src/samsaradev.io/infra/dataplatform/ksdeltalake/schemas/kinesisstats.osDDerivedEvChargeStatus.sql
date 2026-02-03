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
    `ev_charge_status_info`: STRUCT<
      `device_timezone`: STRING,
      `statuses`: STRUCT<
        `complete`: BOOLEAN,
        `missed_target_charge`: BOOLEAN,
        `expected_to_miss_target_charge`: BOOLEAN,
        `charging`: BOOLEAN,
        `low`: BOOLEAN,
        `late_for_scheduled_charge`: BOOLEAN
      >,
      `highest_priority_charge_status_changed_at_ms`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
