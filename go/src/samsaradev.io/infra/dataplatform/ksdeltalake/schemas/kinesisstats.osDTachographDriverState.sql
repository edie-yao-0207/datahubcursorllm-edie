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
    `tachograph_driver_state`: STRUCT<
      `driver_1`: STRUCT<
        `working_state`: INT,
        `card_number`: STRING,
        `issuing_country`: INT,
        `card_ejected`: BOOLEAN,
        `time_related_state`: INT,
        `driving_rest_times`: STRUCT<
          `is_reported`: BOOLEAN,
          `remaining_current_driving_time_minute`: BIGINT,
          `remaining_time_until_next_break_or_rest_minute`: BIGINT,
          `duration_of_next_break_rest_minute`: BIGINT,
          `remaining_time_of_current_break_rest_minute`: BIGINT,
          `time_left_until_next_driving_period_minute`: BIGINT,
          `duration_of_next_driving_period_minute`: BIGINT,
          `current_daily_driving_time_minute`: BIGINT,
          `time_left_until_new_daily_rest_period_minute`: BIGINT,
          `minimum_daily_rest_minute`: BIGINT,
          `remaining_driving_time_of_current_week_minute`: BIGINT,
          `time_left_until_new_weekly_rest_period_minute`: BIGINT,
          `minimum_weekly_rest_minute`: BIGINT,
          `open_compensation_in_the_last_week_minute`: BIGINT,
          `open_compensation_in_week_before_last_minute`: BIGINT,
          `open_compensation_in_second_week_before_last_minute`: BIGINT,
          `end_of_last_daily_rest_period`: STRUCT<
            `timestamp_unix_secs`: BIGINT,
            `local_offset_secs`: INT
          >,
          `maximum_daily_driving_time_minute`: BIGINT,
          `number_of_times_9h_daily_driving_times_exceeded`: BIGINT,
          `number_of_used_reduced_daily_rest_periods`: BIGINT,
          `remaining_2_weeks_driving_time`: BIGINT
        >
      >,
      `driver_2`: STRUCT<
        `working_state`: INT,
        `card_number`: STRING,
        `issuing_country`: INT,
        `card_ejected`: BOOLEAN,
        `time_related_state`: INT,
        `driving_rest_times`: STRUCT<
          `is_reported`: BOOLEAN,
          `remaining_current_driving_time_minute`: BIGINT,
          `remaining_time_until_next_break_or_rest_minute`: BIGINT,
          `duration_of_next_break_rest_minute`: BIGINT,
          `remaining_time_of_current_break_rest_minute`: BIGINT,
          `time_left_until_next_driving_period_minute`: BIGINT,
          `duration_of_next_driving_period_minute`: BIGINT,
          `current_daily_driving_time_minute`: BIGINT,
          `time_left_until_new_daily_rest_period_minute`: BIGINT,
          `minimum_daily_rest_minute`: BIGINT,
          `remaining_driving_time_of_current_week_minute`: BIGINT,
          `time_left_until_new_weekly_rest_period_minute`: BIGINT,
          `minimum_weekly_rest_minute`: BIGINT,
          `open_compensation_in_the_last_week_minute`: BIGINT,
          `open_compensation_in_week_before_last_minute`: BIGINT,
          `open_compensation_in_second_week_before_last_minute`: BIGINT,
          `end_of_last_daily_rest_period`: STRUCT<
            `timestamp_unix_secs`: BIGINT,
            `local_offset_secs`: INT
          >,
          `maximum_daily_driving_time_minute`: BIGINT,
          `number_of_times_9h_daily_driving_times_exceeded`: BIGINT,
          `number_of_used_reduced_daily_rest_periods`: BIGINT,
          `remaining_2_weeks_driving_time`: BIGINT
        >
      >,
      `time_date`: STRUCT<
        `timestamp_unix_secs`: BIGINT,
        `local_offset_secs`: INT
      >,
      `is_from_serial`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
