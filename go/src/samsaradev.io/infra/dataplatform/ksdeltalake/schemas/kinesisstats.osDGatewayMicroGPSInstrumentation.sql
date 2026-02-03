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
    `gateway_micro_gps_instrumentation`: STRUCT<
      `satellites_at_end`: ARRAY<
        STRUCT<
          `prn`: BIGINT,
          `snr`: INT,
          `secs_since_seen`: BIGINT
        >
      >,
      `gps_fixes`: ARRAY<
        STRUCT<
          `secs_since_boot`: BIGINT,
          `secs_since_first_fix`: BIGINT,
          `secs_since_last_fix`: BIGINT,
          `latitude_nd`: BIGINT,
          `longitude_nd`: BIGINT,
          `pdop_thousandths`: BIGINT,
          `hdop_thousandths`: BIGINT,
          `vdop_thousandths`: BIGINT,
          `number_of_satellites_fix`: BIGINT,
          `number_of_satellites_in_view`: BIGINT,
          `satellites`: ARRAY<
            STRUCT<
              `prn`: BIGINT,
              `snr`: INT,
              `secs_since_seen`: BIGINT
            >
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
