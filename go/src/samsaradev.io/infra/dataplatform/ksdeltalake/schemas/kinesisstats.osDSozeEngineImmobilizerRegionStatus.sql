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
    `soze_engine_immobilizer_region_status`: STRUCT<
      `latitude_nd`: BIGINT,
      `longitude_nd`: BIGINT,
      `mcc`: BIGINT,
      `mcc_state`: INT,
      `geo_lock_state`: INT,
      `time_since_last_gps_fix_ms`: BIGINT,
      `gps_last_fix_accuracy_mm`: BIGINT,
      `region_status_reason`: INT,
      `region_auto_immobilization_allowed`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
