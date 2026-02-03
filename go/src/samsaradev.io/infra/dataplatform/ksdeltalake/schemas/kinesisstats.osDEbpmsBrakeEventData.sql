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
    `ebpms_brake_event_data`: STRUCT<
      `event_duration_ms`: BIGINT,
      `tractor_signals`: STRUCT<
        `can_demand_source`: INT,
        `can_demand_pressure`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `pressure_pa`: BIGINT
          >
        >,
        `retarder_source`: INT,
        `retarder_status`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `status`: INT
          >
        >,
        `wheel_speed_source`: INT,
        `wheel_speed`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `speed_meters_per_hour`: BIGINT
          >
        >,
        `delivered_pressure_source`: INT,
        `delivered_pressure_front_axle_left_wheel`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `pressure_pa`: BIGINT
          >
        >,
        `delivered_pressure_front_axle_right_wheel`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `pressure_pa`: BIGINT
          >
        >,
        `delivered_pressure_rear_axle_1_left_wheel`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `pressure_pa`: BIGINT
          >
        >,
        `delivered_pressure_rear_axle_1_right_wheel`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `pressure_pa`: BIGINT
          >
        >,
        `delivered_pressure_rear_axle_2_left_wheel`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `pressure_pa`: BIGINT
          >
        >,
        `delivered_pressure_rear_axle_2_right_wheel`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `pressure_pa`: BIGINT
          >
        >,
        `delivered_pressure_rear_axle_3_left_wheel`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `pressure_pa`: BIGINT
          >
        >,
        `delivered_pressure_rear_axle_3_right_wheel`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `pressure_pa`: BIGINT
          >
        >,
        `pedal_position_source`: INT,
        `pedal_position`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `decipercent`: DECIMAL(20, 0)
          >
        >
      >,
      `trailer_signals`: STRUCT<
        `wheel_speed_source`: INT,
        `wheel_speed`: ARRAY<
          STRUCT<
            `offset_from_start_ms`: INT,
            `speed_meters_per_hour`: BIGINT
          >
        >
      >,
      `ebs_dbg_msgs`: ARRAY<
        STRUCT<
          `offset_from_start_ms`: INT,
          `msg_id`: BIGINT,
          `payload`: BINARY
        >
      >,
      `gps`: ARRAY<
        STRUCT<
          `offset_from_start_ms`: INT,
          `latitude_nanodeg`: BIGINT,
          `longitude_nanodeg`: BIGINT,
          `hdop_thousandths`: BIGINT,
          `vdop_thousandths`: BIGINT,
          `accuracy_mm`: BIGINT,
          `speed_mm_per_s`: BIGINT,
          `altitude_mm`: INT,
          `altitude_accuracy_mm`: BIGINT,
          `num_sats_used_in_fix`: BIGINT
        >
      >,
      `axle_load_kg`: BIGINT,
      `red_warn_lamp`: INT,
      `amber_warn_lamp`: INT,
      `supply_pressure_status`: INT,
      `tractor_abs_ctrl_status`: INT,
      `trailer_abs_ctrl_status`: INT,
      `ecu_id`: BIGINT,
      `event_segmentation`: STRUCT<
        `segment_state`: INT,
        `start_event_time_ms`: BIGINT,
        `segment_counter`: BIGINT
      >,
      `tractor_asr_ctrl_status`: INT,
      `gross_combination_vehicle_weight_kg`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
