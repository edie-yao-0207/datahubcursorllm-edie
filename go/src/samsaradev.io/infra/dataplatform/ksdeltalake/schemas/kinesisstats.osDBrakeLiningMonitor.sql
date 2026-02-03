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
    `brake_lining_monitor`: STRUCT<
      `ecu_id`: BIGINT,
      `wheel_end`: ARRAY<
        STRUCT<
          `axle_from_front`: BIGINT,
          `axle_side`: INT,
          `has_pad_remaining_millipct`: BOOLEAN,
          `pad_remaining_millipct`: BIGINT,
          `brake_lining_sensor_status`: INT,
          `brake_lining_status`: INT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
