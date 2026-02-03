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
    `nordic_gps_debug`: ARRAY<
      STRUCT<
        `gps_fix_info`: STRUCT<
          `latitude_nd`: BIGINT,
          `longitude_nd`: BIGINT,
          `altitude_mm`: INT,
          `accuracy_mm`: BIGINT,
          `speed_mm_per_s`: BIGINT,
          `heading_md`: BIGINT,
          `pdop_thousandths`: BIGINT,
          `hdop_thousandths`: BIGINT,
          `vdop_thousandths`: BIGINT,
          `tdop_thousandths`: BIGINT,
          `time_to_fix_ms`: BIGINT,
          `speed_accuracy_mm_per_s`: BIGINT,
          `vertical_speed_mm_per_s`: INT,
          `vertical_speed_accuracy_mm_per_s`: BIGINT,
          `heading_accuracy_md`: BIGINT,
          `altitude_accuracy_mm`: BIGINT,
          `flags`: BIGINT,
          `execution_time_ms`: BIGINT,
          `fix_index`: INT
        >,
        `no_fix`: BOOLEAN,
        `satellite_infos`: ARRAY<
          STRUCT<
            `sv_id`: BIGINT,
            `cn0_tenths_db_per_hz`: BIGINT,
            `elevation_angle_deg`: BIGINT,
            `azimuth_angle_deg`: BIGINT,
            `used_in_gps_fix`: BOOLEAN
          >
        >,
        `gps_scan_duration_ms`: BIGINT,
        `reported_to_backend`: BOOLEAN,
        `system_state`: INT,
        `filter`: STRUCT<
          `num_filtered_fixes`: BIGINT,
          `time_to_first_filtered_fix_ms`: BIGINT,
          `reason`: INT
        >,
        `location_snap`: STRUCT<
          `cui`: BIGINT,
          `haversine_distance_mm`: BIGINT,
          `distance_mm`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
