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
    `marathon_gnss_debug`: STRUCT<
      `search_cui`: BIGINT,
      `satellite_infos`: ARRAY<
        STRUCT<
          `constellation`: INT,
          `sv_id`: BIGINT,
          `cn0_tenths_db_per_hz`: BIGINT,
          `elevation_angle_deg`: BIGINT,
          `azimuth_angle_deg`: BIGINT,
          `used_in_gps_fix`: BOOLEAN
        >
      >,
      `satellites_seen`: BIGINT,
      `satellites_used`: BIGINT,
      `gnss_fix`: STRUCT<
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
        `speed_accuracy_mm_per_s`: BIGINT,
        `vertical_speed_mm_per_s`: INT,
        `vertical_speed_accuracy_mm_per_s`: BIGINT,
        `heading_accuracy_md`: BIGINT,
        `altitude_accuracy_mm`: BIGINT
      >,
      `time_since_search_start_ms`: BIGINT,
      `fix_type`: INT,
      `receiver_specific_flags`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
