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
    `reversing_event`: STRUCT<
      `duration_ms`: DECIMAL(20, 0),
      `distance_traveled_m`: DECIMAL(20, 0),
      `max_speed_kmph`: DECIMAL(20, 0),
      `time_since_last_forward_move_secs`: DECIMAL(20, 0),
      `time_since_last_forward_move_valid`: BOOLEAN,
      `time_since_state_is_known_secs`: DECIMAL(20, 0),
      `max_invalid_speed_kmph`: DECIMAL(20, 0),
      `max_invalid_speed_at_elapsed_ms`: DECIMAL(20, 0),
      `invalid_speed_readings_count`: DECIMAL(20, 0),
      `location_start`: STRUCT<
        `latitude_nano_deg`: BIGINT,
        `longitude_nano_deg`: BIGINT,
        `accuracy_valid`: BOOLEAN,
        `accuracy_m`: BIGINT
      >,
      `location_end`: STRUCT<
        `latitude_nano_deg`: BIGINT,
        `longitude_nano_deg`: BIGINT,
        `accuracy_valid`: BOOLEAN,
        `accuracy_m`: BIGINT
      >,
      `event_end_reason`: INT,
      `longest_time_between_speed_updates_ms`: DECIMAL(20, 0),
      `longest_time_between_gear_updates_ms`: DECIMAL(20, 0),
      `forward_to_reverse_transition_count`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
