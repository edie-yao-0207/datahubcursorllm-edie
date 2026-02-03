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
    `wheel_end_health`: STRUCT<
      `wheel_ends`: ARRAY<
        STRUCT<
          `wheel_end_position`: STRUCT<
            `axle_from_front`: BIGINT,
            `wheel_end_side`: INT
          >,
          `wheel_end_reading`: STRUCT<
            `temperature_milli_celsius`: INT,
            `temperature_warning_active`: BOOLEAN,
            `temperature_sensor_failure_active`: BOOLEAN,
            `wheel_end_failure_status`: INT,
            `sensor_battery_level_status`: INT,
            `vibration_sensor_failure_status`: INT
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
