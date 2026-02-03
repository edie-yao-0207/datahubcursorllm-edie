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
    `storage_metrics`: STRUCT<
      `storage_device`: ARRAY<
        STRUCT<
          `model`: STRING,
          `serial`: STRING,
          `total_bytes`: BIGINT,
          `free_bytes`: BIGINT,
          `smart_attributes`: STRUCT<
            `valid_stats_bitmap`: BIGINT,
            `reallocated_sector_count`: BIGINT,
            `power_on_hours`: BIGINT,
            `power_cycle_count`: BIGINT,
            `wear_leveling_count`: BIGINT,
            `used_reserved_block_count_total`: BIGINT,
            `program_fail_count_total`: BIGINT,
            `erase_fail_count_total`: BIGINT,
            `runtime_bad_block`: BIGINT,
            `uncorrectable_error_count`: BIGINT,
            `airflow_temperature_degree_c`: INT,
            `ecc_error_rate`: BIGINT,
            `crc_error_count`: BIGINT,
            `por_recovery_count`: BIGINT,
            `total_lbas_read`: BIGINT,
            `total_lbas_written`: BIGINT,
            `temperature_degree_c`: INT
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
