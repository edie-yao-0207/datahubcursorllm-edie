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
    `immobilizer_event`: STRUCT<
      `source_objectstat_timestamp_ms`: DECIMAL(20, 0),
      `context`: STRUCT<
        `location`: STRUCT<
          `latitude`: DOUBLE,
          `longitude`: DOUBLE
        >,
        `driver_id`: BIGINT,
        `user_id`: BIGINT,
        `immobilizer_type`: INT,
        `vg_firmware_version`: STRING
      >,
      `relays_events`: ARRAY<
        STRUCT<
          `relay_id`: BIGINT,
          `operation_type`: INT,
          `source`: INT,
          `correlation_id`: STRING,
          `change_reason`: INT,
          `status`: INT,
          `command_uuid`: STRING
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
