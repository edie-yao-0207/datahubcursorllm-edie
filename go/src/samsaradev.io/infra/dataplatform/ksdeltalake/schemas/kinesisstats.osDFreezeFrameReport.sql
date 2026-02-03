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
    `freeze_frame_report`: STRUCT<
      `j1939_freeze_frame_data`: ARRAY<
        STRUCT<
          `ecu_id`: BIGINT,
          `spn`: BIGINT,
          `fmi`: BIGINT,
          `spn_version_legacy`: BOOLEAN,
          `occurrence_count`: BIGINT,
          `parameters`: ARRAY<
            STRUCT<
              `spn`: BIGINT,
              `data`: BINARY
            >
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
