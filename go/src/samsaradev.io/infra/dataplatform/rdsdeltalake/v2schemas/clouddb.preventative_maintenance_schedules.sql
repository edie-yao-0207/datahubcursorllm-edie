`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`group_id` BIGINT,
`title` STRING,
`description` STRING,
`marshaled_proto` STRUCT<
  `interval`: STRUCT<
    `distance_interval`: STRUCT<
      `distance_miles`: BIGINT,
      `vehicleSchedules`: ARRAY<
        STRUCT<
          `id`: BIGINT,
          `next_odometer_miles`: BIGINT,
          `prev_log_id`: STRUCT<`value`: BIGINT>
        >
      >
    >,
    `date_interval`: STRUCT<
      `dates`: BIGINT,
      `vehicleSchedules`: ARRAY<
        STRUCT<
          `id`: BIGINT,
          `next_time_ms`: BIGINT,
          `prev_log_id`: STRUCT<`value`: BIGINT>
        >
      >
    >,
    `engine_hour_interval`: STRUCT<
      `engine_hours`: BIGINT,
      `vehicleSchedules`: ARRAY<
        STRUCT<
          `id`: BIGINT,
          `next_engine_hours`: BIGINT,
          `prev_log_id`: STRUCT<`value`: BIGINT>
        >
      >
    >
  >,
  `work_order_template_id`: STRING,
  `linked_schedule_ids`: ARRAY<BIGINT>
>,
`_raw_marshaled_proto` STRING,
`partition` STRING
