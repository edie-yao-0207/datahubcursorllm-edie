`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`org_id` BIGINT,
`route_id` BIGINT,
`audit_proto` STRUCT<
  `id`: BIGINT,
  `state_machine`: STRUCT<
    `route`: STRUCT<
      `id`: BIGINT,
      `name`: STRING,
      `group_id`: BIGINT,
      `vehicle_id`: BIGINT,
      `driver_id`: BIGINT,
      `timezone`: STRING,
      `scheduled_start_ms`: BIGINT,
      `scheduled_end_ms`: BIGINT,
      `start_location_name`: STRING,
      `start_location_address`: STRING,
      `start_location_latitude`: DOUBLE,
      `start_location_longitude`: DOUBLE,
      `parent_recurring_route_id`: BIGINT,
      `has_diverged_from_parent`: BOOLEAN,
      `custom_fields_json`: STRING,
      `start_location_address_id`: BIGINT,
      `scheduled_meters`: BIGINT,
      `notes`: STRING,
      `org_id`: BIGINT,
      `last_processed_location_ms`: BIGINT,
      `complete_last_stop_on_departure`: BOOLEAN,
      `trailer_device_id`: BIGINT,
      `llm_structured_notes`: STRING,
      `tag_ids`: ARRAY<BIGINT>,
      `hub_uuid`: BINARY,
      `equipment_profile_uuid`: BINARY,
      `estimated_cost`: DOUBLE,
      `start_location_radius_meters`: DOUBLE,
      `sorted_job_ids`: ARRAY<BIGINT>,
      `sequencing_method`: INT,
      `navigation_source`: INT
    >,
    `before_state`: STRUCT<
      `jobs`: ARRAY<
        STRUCT<
          `id`: BIGINT,
          `org_id`: BIGINT,
          `group_id`: BIGINT,
          `driver_id`: BIGINT,
          `vehicle_id`: BIGINT,
          `dispatch_route_id`: BIGINT,
          `job_status`: STRUCT<
            `job_state`: INT,
            `en_route_ms`: BIGINT,
            `arrived_ms`: BIGINT,
            `completed_ms`: BIGINT,
            `skipped_ms`: BIGINT
          >,
          `time_window`: STRUCT<
            `scheduled_arrival_time`: BIGINT,
            `job_skip_threshold_ms`: BIGINT,
            `scheduled_departure_time`: BIGINT
          >,
          `notes`: STRING,
          `destination`: STRUCT<
            `name`: STRING,
            `address`: STRING,
            `location`: STRUCT<
              `latitude`: DOUBLE,
              `longitude`: DOUBLE
            >,
            `address_id`: BIGINT,
            `timezone`: STRING,
            `radius_meters`: DOUBLE
          >,
          `external_identifier`: STRING,
          `created_at`: BIGINT,
          `created_by`: BIGINT,
          `eta`: STRUCT<
            `eta_ms`: BIGINT,
            `eta_refreshed_ms`: BIGINT,
            `eta_refreshed_latitude`: DOUBLE,
            `eta_refreshed_longitude`: DOUBLE
          >,
          `fleet_viewer_token`: STRING,
          `notes_extended`: STRING,
          `eta_refreshed_count`: BIGINT,
          `ontime_window_before_arrival_ms`: DECIMAL(20, 0),
          `ontime_window_after_arrival_ms`: DECIMAL(20, 0),
          `llm_structured_notes`: STRING,
          `skipped_tasks_count`: BIGINT,
          `custom_properties`: ARRAY<
            STRUCT<
              `key`: STRING,
              `value`: STRUCT<
                `type`: INT,
                `string_values`: ARRAY<STRING>,
                `integer_values`: ARRAY<BIGINT>,
                `double_values`: ARRAY<DOUBLE>,
                `boolean_values`: ARRAY<BOOLEAN>
              >
            >
          >,
          `location_uuid`: BINARY,
          `exceptions`: STRUCT<
            `skipped_tasks_count`: BIGINT,
            `arrival_prevented`: BOOLEAN,
            `departure_prevented`: BOOLEAN
          >,
          `sequence_number`: BIGINT
        >
      >
    >,
    `after_state`: STRUCT<
      `jobs`: ARRAY<
        STRUCT<
          `id`: BIGINT,
          `org_id`: BIGINT,
          `group_id`: BIGINT,
          `driver_id`: BIGINT,
          `vehicle_id`: BIGINT,
          `dispatch_route_id`: BIGINT,
          `job_status`: STRUCT<
            `job_state`: INT,
            `en_route_ms`: BIGINT,
            `arrived_ms`: BIGINT,
            `completed_ms`: BIGINT,
            `skipped_ms`: BIGINT
          >,
          `time_window`: STRUCT<
            `scheduled_arrival_time`: BIGINT,
            `job_skip_threshold_ms`: BIGINT,
            `scheduled_departure_time`: BIGINT
          >,
          `notes`: STRING,
          `destination`: STRUCT<
            `name`: STRING,
            `address`: STRING,
            `location`: STRUCT<
              `latitude`: DOUBLE,
              `longitude`: DOUBLE
            >,
            `address_id`: BIGINT,
            `timezone`: STRING,
            `radius_meters`: DOUBLE
          >,
          `external_identifier`: STRING,
          `created_at`: BIGINT,
          `created_by`: BIGINT,
          `eta`: STRUCT<
            `eta_ms`: BIGINT,
            `eta_refreshed_ms`: BIGINT,
            `eta_refreshed_latitude`: DOUBLE,
            `eta_refreshed_longitude`: DOUBLE
          >,
          `fleet_viewer_token`: STRING,
          `notes_extended`: STRING,
          `eta_refreshed_count`: BIGINT,
          `ontime_window_before_arrival_ms`: DECIMAL(20, 0),
          `ontime_window_after_arrival_ms`: DECIMAL(20, 0),
          `llm_structured_notes`: STRING,
          `skipped_tasks_count`: BIGINT,
          `custom_properties`: ARRAY<
            STRUCT<
              `key`: STRING,
              `value`: STRUCT<
                `type`: INT,
                `string_values`: ARRAY<STRING>,
                `integer_values`: ARRAY<BIGINT>,
                `double_values`: ARRAY<DOUBLE>,
                `boolean_values`: ARRAY<BOOLEAN>
              >
            >
          >,
          `location_uuid`: BINARY,
          `exceptions`: STRUCT<
            `skipped_tasks_count`: BIGINT,
            `arrival_prevented`: BOOLEAN,
            `departure_prevented`: BOOLEAN
          >,
          `sequence_number`: BIGINT
        >
      >
    >,
    `processed_events`: ARRAY<
      STRUCT<
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
      >
    >,
    `pending_events`: ARRAY<
      STRUCT<
        `event`: STRUCT<
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
        `trigger`: INT,
        `resolving_event`: STRUCT<
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
        `dismissing_event`: STRUCT<
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
        >
      >
    >
  >,
  `changed_at_ms`: BIGINT,
  `org_id`: BIGINT,
  `processing_metadata`: STRUCT<
    `enforce_stop_order`: STRUCT<
      `enabled`: BOOLEAN,
      `allow_first_stop_skip`: BOOLEAN
    >
  >
>,
`_raw_audit_proto` STRING,
`created_at` TIMESTAMP,
`date` STRING
