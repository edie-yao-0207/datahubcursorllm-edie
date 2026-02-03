`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `accel_event_data`: STRUCT<
    `recent_accel`: ARRAY<
      STRUCT<
        `x_f`: FLOAT,
        `y_f`: FLOAT,
        `z_f`: FLOAT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `sample_rate_hz`: DOUBLE,
    `recent_gps`: ARRAY<
      STRUCT<
        `latitude`: BIGINT,
        `longitude`: BIGINT,
        `speed`: BIGINT,
        `heading`: BIGINT,
        `fix`: BOOLEAN,
        `altitude`: INT,
        `hdop`: BIGINT,
        `vdop`: BIGINT,
        `ts`: BIGINT,
        `event_offset_ms`: BIGINT,
        `ecu_speed`: BIGINT,
        `ecu_speed_valid`: BOOLEAN
      >
    >,
    `is_debug`: BOOLEAN,
    `accel_pedal`: ARRAY<
      STRUCT<
        `position`: BIGINT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `brake`: ARRAY<
      STRUCT<
        `brake_on`: BOOLEAN,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `recent_gyro`: ARRAY<
      STRUCT<
        `x_dps`: FLOAT,
        `y_dps`: FLOAT,
        `z_dps`: FLOAT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `ecu_speed`: ARRAY<
      STRUCT<
        `speed_milliknots`: BIGINT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `cruise_control`: ARRAY<
      STRUCT<
        `cruise_control_activated`: BOOLEAN,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `forward_vehicle_speed`: ARRAY<
      STRUCT<
        `speed_milliknots`: BIGINT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `forward_vehicle_distance`: ARRAY<
      STRUCT<
        `distance_meters`: BIGINT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `oriented_harsh_detector_triggered`: BOOLEAN,
    `oriented_accel_raw`: ARRAY<
      STRUCT<
        `x_f`: FLOAT,
        `y_f`: FLOAT,
        `z_f`: FLOAT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `oriented_gyro_raw`: ARRAY<
      STRUCT<
        `x_dps`: FLOAT,
        `y_dps`: FLOAT,
        `z_dps`: FLOAT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `oriented_accel_lp_filtered`: ARRAY<
      STRUCT<
        `x_f`: FLOAT,
        `y_f`: FLOAT,
        `z_f`: FLOAT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `oriented_gyro_lp_filtered`: ARRAY<
      STRUCT<
        `x_dps`: FLOAT,
        `y_dps`: FLOAT,
        `z_dps`: FLOAT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `ecu_distance_alert_signal`: ARRAY<
      STRUCT<
        `state`: INT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `ecu_forward_collision_warning`: ARRAY<
      STRUCT<
        `state`: INT,
        `offset_from_event_ms`: BIGINT
      >
    >,
    `ecu_external_acceleration_demand`: ARRAY<
      STRUCT<
        `acceleration_demand_mg`: BIGINT,
        `offset_from_event_ms`: BIGINT
      >
    >
  >
>,
`_synced_at` TIMESTAMP
