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
    `jamming_detection`: STRUCT<
      `source_states`: ARRAY<
        STRUCT<
          `source`: INT,
          `state`: INT,
          `last_change_at_ms`: BIGINT
        >
      >,
      `last_location`: STRUCT<
        `location_time_unix_ms`: BIGINT,
        `latitude_nano_degrees`: BIGINT,
        `longitude_nano_degrees`: BIGINT,
        `accuracy_mm`: BIGINT,
        `gps_speed_kmph`: BIGINT
      >,
      `ecu_speed_kmph`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
