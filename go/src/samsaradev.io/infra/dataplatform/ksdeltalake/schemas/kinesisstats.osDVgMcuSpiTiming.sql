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
    `vg_mcu_spi_timing_info`: STRUCT<
      `messages_received`: BIGINT,
      `messages_dropped`: BIGINT,
      `median_spi_transfer_ms`: BIGINT,
      `max_spi_transfer_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
