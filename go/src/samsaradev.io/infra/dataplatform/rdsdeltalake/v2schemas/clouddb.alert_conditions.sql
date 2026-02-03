`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`alert_id` BIGINT,
`type` INT,
`int_value1` BIGINT,
`int_value2` BIGINT,
`proto_data` STRUCT<
  `geofence_circle`: STRUCT<
    `name`: STRING,
    `radius_meters`: DOUBLE,
    `center_lat`: DOUBLE,
    `center_lng`: DOUBLE
  >,
  `geofence_polygon`: STRUCT<
    `name`: STRING,
    `vertices`: ARRAY<
      STRUCT<
        `lat`: DOUBLE,
        `lng`: DOUBLE
      >
    >,
    `bounding_box`: STRUCT<
      `lat_max`: DOUBLE,
      `lng_max`: DOUBLE,
      `lat_min`: DOUBLE,
      `lng_min`: DOUBLE
    >
  >,
  `dispatch_eta`: STRUCT<
    `name`: STRING,
    `address`: STRING,
    `latitude`: DOUBLE,
    `longitude`: DOUBLE,
    `eta_threshold_ms`: BIGINT,
    `enable_fleet_viewer_link`: BOOLEAN
  >,
  `machine_input_threshold`: DOUBLE,
  `maintenance_schedule`: STRUCT<
    `maintenance_schedule_id`: BIGINT,
    `maintenance_time_threshold_ms`: BIGINT
  >,
  `maintenance_schedule_engine_hours`: STRUCT<
    `maintenance_schedule_id`: BIGINT,
    `maintenance_engine_hours_threshold_ms`: BIGINT
  >,
  `maintenance_schedule_odometer`: STRUCT<
    `maintenance_schedule_id`: BIGINT,
    `maintenance_odometer_threshold_meters`: BIGINT
  >,
  `disabled_time_range`: STRUCT<
    `days_of_week`: ARRAY<INT>,
    `start_hour`: BIGINT,
    `start_minute`: BIGINT,
    `end_hour`: BIGINT,
    `end_minute`: BIGINT
  >,
  `harshEventTypes`: ARRAY<INT>,
  `geofence_address`: STRUCT<
    `address_id`: BIGINT,
    `address_types`: ARRAY<BIGINT>
  >,
  `document_submission`: STRUCT<
    `template_uuids`: ARRAY<STRING>
  >,
  `speeding`: STRUCT<`speeding_type`: INT>,
  `machine_input_bounds`: STRUCT<
    `upper`: DOUBLE,
    `lower`: DOUBLE
  >,
  `onvif_camera_info_list`: STRUCT<
    `camera_info_list`: ARRAY<
      STRUCT<
        `camera_uuid`: BINARY,
        `camera_name`: STRING,
        `stream_uid`: BINARY
      >
    >
  >,
  `onvif_camera_stream_areas_of_interest_list`: STRUCT<
    `onvif_camera_stream_areas_of_interest`: ARRAY<
      STRUCT<
        `camera_uuid`: BINARY,
        `stream_uid`: BINARY,
        `vms_id`: BIGINT,
        `area_of_interest_infos`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `name`: STRING,
            `uuid`: BINARY
          >
        >
      >
    >
  >,
  `vision_actions`: STRUCT<
    `action_types`: ARRAY<INT>
  >,
  `temperature`: STRUCT<
    `cargo_is_full`: BIGINT,
    `door_is_closed`: BIGINT
  >,
  `vision_metrics`: STRUCT<`cpu_threshold`: DOUBLE>,
  `hos_violation_configuration_list`: STRUCT<
    `hos_violation_configuration`: ARRAY<
      STRUCT<
        `hos_alert_type`: INT,
        `ms_in_advance`: BIGINT
      >
    >
  >,
  `enabled_time_range`: STRUCT<
    `days_of_week`: ARRAY<INT>,
    `start_hour`: BIGINT,
    `start_minute`: BIGINT,
    `end_hour`: BIGINT,
    `end_minute`: BIGINT
  >,
  `prioritized_vehicle_fault_codes`: STRUCT<
    `vehicle_fault_codes`: ARRAY<
      STRUCT<
        `fault_code`: STRING,
        `type`: INT
      >
    >,
    `red_lamp`: BOOLEAN,
    `mil`: BOOLEAN,
    `amber_lamp`: BOOLEAN,
    `protection_lamp`: BOOLEAN
  >,
  `tire_fault_codes`: STRUCT<
    `all_critical`: BOOLEAN,
    `all_noncritical`: BOOLEAN,
    `tire_fault_codes`: ARRAY<INT>
  >,
  `panic_button`: STRUCT<`filter_out_power_loss`: BOOLEAN>,
  `is_aggressive_alert`: BOOLEAN,
  `reefer_alarm`: STRUCT<`ignore_passive_alarms`: BOOLEAN>,
  `measure`: STRUCT<
    `numeric`: STRUCT<`operation`: INT>,
    `inverse`: BOOLEAN
  >,
  `custom_digio`: STRUCT<`uuid`: STRING>
>,
`_raw_proto_data` STRING,
`created_at` TIMESTAMP,
`alert_subcondition_uuid` STRING,
`partition` STRING
