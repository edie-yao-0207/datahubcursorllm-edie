`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`dispatch_route_id` BIGINT,
`event_proto` STRUCT<
  `clock_event`: STRUCT<`evaluated_at_ms`: BIGINT>,
  `validation_event`: STRUCT<`evaluated_at_ms`: BIGINT>,
  `vehicle_stopped_event`: STRUCT<
    `stopLocation`: STRUCT<
      `location`: STRUCT<
        `latitude`: DOUBLE,
        `longitude`: DOUBLE
      >,
      `timeMs`: BIGINT
    >
  >,
  `vehicle_moving_event`: STRUCT<
    `startLocation`: STRUCT<
      `location`: STRUCT<
        `latitude`: DOUBLE,
        `longitude`: DOUBLE
      >,
      `timeMs`: BIGINT
    >
  >,
  `admin_arrival_event`: STRUCT<
    `arrived_at_ms`: BIGINT,
    `dispatch_job_id`: BIGINT,
    `notes`: STRING,
    `server_created_at_ms`: BIGINT
  >,
  `admin_completion_event`: STRUCT<
    `completed_at_ms`: BIGINT,
    `dispatch_job_id`: BIGINT,
    `notes`: STRING,
    `server_created_at_ms`: BIGINT
  >,
  `driver_arrival_event`: STRUCT<
    `arrived_at_ms`: BIGINT,
    `dispatch_job_id`: BIGINT,
    `server_created_at_ms`: BIGINT,
    `client_created_at_ms`: BIGINT
  >,
  `driver_completion_event`: STRUCT<
    `completed_at_ms`: BIGINT,
    `dispatch_job_id`: BIGINT,
    `server_created_at_ms`: BIGINT,
    `client_created_at_ms`: BIGINT
  >,
  `geofence_entry_event`: STRUCT<
    `entry_location`: STRUCT<
      `location`: STRUCT<
        `latitude`: DOUBLE,
        `longitude`: DOUBLE
      >,
      `timeMs`: BIGINT
    >,
    `dispatch_job_id`: BIGINT,
    `location_at_entry_threshold`: STRUCT<
      `latitude`: DOUBLE,
      `longitude`: DOUBLE
    >
  >,
  `geofence_exit_event`: STRUCT<
    `exit_location`: STRUCT<
      `location`: STRUCT<
        `latitude`: DOUBLE,
        `longitude`: DOUBLE
      >,
      `timeMs`: BIGINT
    >,
    `dispatch_job_id`: BIGINT
  >,
  `driver_dismiss_pending_event`: STRUCT<
    `trigger`: INT,
    `dispatch_job_id`: BIGINT,
    `server_created_at_ms`: BIGINT,
    `client_created_at_ms`: BIGINT
  >,
  `driver_confirm_pending_event`: STRUCT<
    `trigger`: INT,
    `dispatch_job_id`: BIGINT,
    `server_created_at_ms`: BIGINT,
    `client_created_at_ms`: BIGINT,
    `notes`: STRING
  >,
  `driver_route_start_event`: STRUCT<
    `server_created_at_ms`: BIGINT,
    `client_created_at_ms`: BIGINT
  >,
  `driver_route_end_event`: STRUCT<
    `server_created_at_ms`: BIGINT,
    `client_created_at_ms`: BIGINT
  >,
  `driver_update_stops_event`: STRUCT<
    `server_created_at_ms`: BIGINT,
    `client_created_at_ms`: BIGINT,
    `updated_jobs`: ARRAY<
      STRUCT<
        `dispatch_job_id`: BIGINT,
        `scheduled_arrival_ms`: BIGINT,
        `scheduled_departure_ms`: BIGINT,
        `skipped`: BOOLEAN
      >
    >,
    `notes`: STRING
  >,
  `mobile_auto_arrival_event`: STRUCT<
    `arrived_at_ms`: BIGINT,
    `dispatch_job_id`: BIGINT,
    `server_created_at_ms`: BIGINT,
    `client_created_at_ms`: BIGINT,
    `triggers`: ARRAY<INT>,
    `location`: STRUCT<
      `latitude`: DOUBLE,
      `longitude`: DOUBLE
    >,
    `distance_to_stop_meters`: DOUBLE
  >,
  `mobile_auto_departure_event`: STRUCT<
    `departed_at_ms`: BIGINT,
    `dispatch_job_id`: BIGINT,
    `server_created_at_ms`: BIGINT,
    `client_created_at_ms`: BIGINT,
    `triggers`: ARRAY<INT>,
    `dwell_time_ms`: BIGINT
  >
>,
`_raw_event_proto` STRING,
`server_created_at` TIMESTAMP,
`date` STRING
