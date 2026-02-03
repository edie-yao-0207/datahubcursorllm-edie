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
    `computed_health_status`: STRUCT<
      `ordered_rule_outputs`: ARRAY<
        STRUCT<
          `rule_code`: INT,
          `status_code`: INT,
          `category`: INT,
          `changed_at_ms`: BIGINT,
          `details`: STRUCT<
            `trip_uptime_details`: STRUCT<
              `overall`: STRUCT<
                `start_time_ms`: BIGINT,
                `end_time_ms`: BIGINT,
                `grace_recording_duration_ms`: BIGINT,
                `trip_duration_ms`: BIGINT,
                `vg_cumulative_bootcount`: BIGINT,
                `vg_earliest_build`: STRING,
                `cm_cumulative_bootcount`: BIGINT,
                `cm_earliest_build`: STRING
              >,
              `segments`: ARRAY<
                STRUCT<
                  `start_time_ms`: BIGINT,
                  `end_time_ms`: BIGINT,
                  `grace_recording_duration_ms`: BIGINT,
                  `trip_duration_ms`: BIGINT,
                  `vg_cumulative_bootcount`: BIGINT,
                  `vg_earliest_build`: STRING,
                  `cm_cumulative_bootcount`: BIGINT,
                  `cm_earliest_build`: STRING
                >
              >
            >,
            `obstruction_details`: STRUCT<
              `latest_inward_detection_ms`: BIGINT,
              `latest_outward_detection_ms`: BIGINT,
              `latest_misaligned_detection_ms`: BIGINT
            >,
            `camera_detection_details`: STRUCT<`camera_last_connected_at_ms`: BIGINT>,
            `gateway_power_details`: STRUCT<`gateway_last_connected_at_ms`: BIGINT>,
            `asset_gateway_battery_details`: STRUCT<
              `gateway_battery_state`: BIGINT,
              `battery_mv`: BIGINT,
              `battery_cell_id_sum_1_2_3_mv`: INT,
              `battery_temperature_mc`: BIGINT,
              `battery_fuel_gauge_soc`: FLOAT,
              `battery_level`: BIGINT
            >,
            `asset_gateway_detection_details`: STRUCT<
              `last_check_in_ms`: BIGINT,
              `check_in_interval_ms`: BIGINT,
              `vehicle_battery_mv`: BIGINT,
              `cell_connectivity`: STRUCT<
                `rssi_dbm`: INT,
                `rsrp_dbm`: INT,
                `operator`: STRING
              >
            >,
            `asset_gateway_gps_signal_details`: STRUCT<
              `last_check_in_ms`: BIGINT,
              `last_gps_fix_location_ms`: BIGINT,
              `check_in_interval_ms`: BIGINT,
              `cell_connectivity`: STRUCT<
                `rssi_dbm`: INT,
                `rsrp_dbm`: INT,
                `operator`: STRING
              >
            >,
            `asset_gateway_plug_details`: STRUCT<
              `cable_id`: BIGINT,
              `cable_change_detected`: INT,
              `vehicle_battery_mv`: BIGINT
            >,
            `recording_details`: STRUCT<`uptime_last_fifty_hours`: DOUBLE>,
            `drive_time_details`: STRUCT<`drive_time_last_thirty_days_ms`: BIGINT>,
            `vehicle_gateway_power_details`: STRUCT<
              `vehicle_battery_mv`: BIGINT,
              `gateway_battery_state`: BIGINT
            >,
            `vehicle_gateway_detection_details`: STRUCT<
              `vehicle_battery_mv`: BIGINT,
              `cell_connectivity`: STRUCT<
                `rssi_dbm`: INT,
                `rsrp_dbm`: INT,
                `operator`: STRING
              >,
              `last_connected_at_ms`: BIGINT,
              `last_check_in_ms`: BIGINT
            >,
            `cm_target_uptime_details`: STRUCT<
              `overall`: STRUCT<
                `start_time_ms`: BIGINT,
                `end_time_ms`: BIGINT,
                `grace_recording_duration_ms`: BIGINT,
                `target_duration_ms`: BIGINT,
                `vg_cumulative_bootcount`: BIGINT,
                `vg_earliest_build`: STRING,
                `cm_cumulative_bootcount`: BIGINT,
                `cm_earliest_build`: STRING
              >,
              `segments`: ARRAY<
                STRUCT<
                  `start_time_ms`: BIGINT,
                  `end_time_ms`: BIGINT,
                  `grace_recording_duration_ms`: BIGINT,
                  `target_duration_ms`: BIGINT,
                  `vg_cumulative_bootcount`: BIGINT,
                  `vg_earliest_build`: STRING,
                  `cm_cumulative_bootcount`: BIGINT,
                  `cm_earliest_build`: STRING
                >
              >
            >,
            `camera_connector_trip_uptime_details`: STRUCT<
              `overall`: STRUCT<
                `start_time_ms`: BIGINT,
                `end_time_ms`: BIGINT,
                `recording_duration_ms`: BIGINT,
                `trip_duration_ms`: BIGINT,
                `connected_duration_ms`: BIGINT,
                `recording_per_reversing_ms`: BIGINT,
                `reversing_ms`: BIGINT,
                `connected_uptime_pct`: DOUBLE,
                `recording_uptime_pct`: DOUBLE,
                `health_category`: INT
              >,
              `segments`: ARRAY<
                STRUCT<
                  `start_time_ms`: BIGINT,
                  `end_time_ms`: BIGINT,
                  `recording_duration_ms`: BIGINT,
                  `trip_duration_ms`: BIGINT,
                  `connected_duration_ms`: BIGINT,
                  `recording_per_reversing_ms`: BIGINT,
                  `reversing_ms`: BIGINT,
                  `connected_uptime_pct`: DOUBLE,
                  `recording_uptime_pct`: DOUBLE,
                  `health_category`: INT
                >
              >,
              `connector_attribute`: INT,
              `reverse_only`: BOOLEAN,
              `has_auxcam`: BOOLEAN
            >,
            `cm_uptime_details`: STRUCT<
              `overall`: STRUCT<
                `start_time_ms`: BIGINT,
                `end_time_ms`: BIGINT,
                `grace_recording_duration_ms`: BIGINT,
                `target_duration_ms`: BIGINT,
                `vg_powered_on_duration_ms`: BIGINT,
                `cm_powered_on_connected_duration_ms`: BIGINT,
                `vg_cumulative_bootcount`: BIGINT,
                `vg_earliest_build`: STRING,
                `cm_cumulative_bootcount`: BIGINT,
                `cm_earliest_build`: STRING,
                `uptime_pct`: DOUBLE,
                `recording_uptime_pct`: DOUBLE,
                `connectivity_uptime_pct`: DOUBLE
              >,
              `segments`: ARRAY<
                STRUCT<
                  `start_time_ms`: BIGINT,
                  `end_time_ms`: BIGINT,
                  `grace_recording_duration_ms`: BIGINT,
                  `target_duration_ms`: BIGINT,
                  `vg_powered_on_duration_ms`: BIGINT,
                  `cm_powered_on_connected_duration_ms`: BIGINT,
                  `vg_cumulative_bootcount`: BIGINT,
                  `vg_earliest_build`: STRING,
                  `cm_cumulative_bootcount`: BIGINT,
                  `cm_earliest_build`: STRING,
                  `uptime_pct`: DOUBLE,
                  `recording_uptime_pct`: DOUBLE,
                  `connectivity_uptime_pct`: DOUBLE
                >
              >
            >,
            `camera_connectors_obstruction_details`: STRUCT<
              `obstruction_details`: ARRAY<
                STRUCT<
                  `obstruction_details`: STRUCT<
                    `latest_inward_detection_ms`: BIGINT,
                    `latest_outward_detection_ms`: BIGINT,
                    `latest_misaligned_detection_ms`: BIGINT
                  >,
                  `media_input`: INT
                >
              >
            >,
            `media_inputs_trip_uptime_details`: STRUCT<
              `uptime_details`: ARRAY<
                STRUCT<
                  `media_input`: INT,
                  `connected_uptime_pct`: DOUBLE,
                  `recording_uptime_pct`: DOUBLE,
                  `health_category`: INT
                >
              >
            >,
            `crux_battery_details`: STRUCT<`gateway_battery_state`: BIGINT>,
            `cc_target_uptime_details`: STRUCT<
              `overall`: STRUCT<
                `start_time_ms`: BIGINT,
                `end_time_ms`: BIGINT,
                `grace_recording_duration_ms`: BIGINT,
                `target_duration_ms`: BIGINT
              >,
              `segments`: ARRAY<
                STRUCT<
                  `start_time_ms`: BIGINT,
                  `end_time_ms`: BIGINT,
                  `grace_recording_duration_ms`: BIGINT,
                  `target_duration_ms`: BIGINT
                >
              >
            >,
            `crux_detection_details`: STRUCT<`last_check_in_ms`: DECIMAL(20, 0)>,
            `auxcam_uptime_details`: STRUCT<
              `overall`: STRUCT<
                `start_time_ms`: BIGINT,
                `end_time_ms`: BIGINT,
                `grace_recording_duration_ms`: BIGINT,
                `target_duration_ms`: BIGINT,
                `vg_powered_on_duration_ms`: BIGINT,
                `auxcam_powered_on_connected_duration_ms`: BIGINT,
                `uptime_pct`: DOUBLE,
                `recording_uptime_pct`: DOUBLE,
                `connectivity_uptime_pct`: DOUBLE
              >,
              `segments`: ARRAY<
                STRUCT<
                  `start_time_ms`: BIGINT,
                  `end_time_ms`: BIGINT,
                  `grace_recording_duration_ms`: BIGINT,
                  `target_duration_ms`: BIGINT,
                  `vg_powered_on_duration_ms`: BIGINT,
                  `auxcam_powered_on_connected_duration_ms`: BIGINT,
                  `uptime_pct`: DOUBLE,
                  `recording_uptime_pct`: DOUBLE,
                  `connectivity_uptime_pct`: DOUBLE
                >
              >,
              `vg_serial`: STRING
            >,
            `camera_install_details`: STRUCT<
              `is_camera_installed`: BOOLEAN,
              `is_vg_installed`: BOOLEAN,
              `camera_id`: BIGINT,
              `vg_id`: BIGINT
            >
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
