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
    `event_based_recording_safety_events_for_clip`: STRUCT<
      `clip_name`: STRING,
      `safety_events`: ARRAY<
        STRUCT<
          `type`: INT,
          `harsh_event`: STRUCT<
            `trigger_time_unix_ms`: DECIMAL(20, 0),
            `harsh_accel_type`: INT,
            `event_id`: DECIMAL(20, 0),
            `gateway_id`: DECIMAL(20, 0)
          >,
          `camera_button_press`: STRUCT<`trigger_time_unix_ms`: DECIMAL(20, 0)>,
          `panic_button_press`: STRUCT<`trigger_time_unix_ms`: DECIMAL(20, 0)>
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
