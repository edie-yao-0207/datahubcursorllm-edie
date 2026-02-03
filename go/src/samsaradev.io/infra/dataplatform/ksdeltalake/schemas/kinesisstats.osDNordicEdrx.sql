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
    `nordic_edrx`: STRUCT<
      `requested`: STRUCT<
        `edrx_interval_ms`: BIGINT,
        `paging_time_window_ms`: BIGINT
      >,
      `received`: STRUCT<
        `edrx_interval_ms`: BIGINT,
        `paging_time_window_ms`: BIGINT
      >,
      `network_rejected`: BOOLEAN,
      `mcc`: BIGINT,
      `mnc`: BIGINT,
      `cell_id`: BIGINT,
      `tac`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
