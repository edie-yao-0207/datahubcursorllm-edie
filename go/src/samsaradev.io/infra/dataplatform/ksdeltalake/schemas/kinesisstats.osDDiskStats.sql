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
    `disk_stats`: STRUCT<
      `ubi_infos`: ARRAY<
        STRUCT<
          `volumes_count`: DECIMAL(20, 0),
          `logical_eraseblock_size`: DECIMAL(20, 0),
          `total_amount_of_logical_eraseblocks`: DECIMAL(20, 0),
          `amount_of_available_logic_eraseblocks`: DECIMAL(20, 0),
          `count_of_bad_physical_eraseblocks`: DECIMAL(20, 0),
          `count_of_reserved_physical_eraseblocks`: DECIMAL(20, 0),
          `current_maximum_erase_counter_value`: DECIMAL(20, 0),
          `minimum_output_unit_size`: DECIMAL(20, 0)
        >
      >,
      `free_space_info_home`: STRUCT<
        `free_space_kbytes`: DECIMAL(20, 0),
        `free_space_percent`: FLOAT
      >,
      `disk_encryption`: STRUCT<`status`: INT>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
