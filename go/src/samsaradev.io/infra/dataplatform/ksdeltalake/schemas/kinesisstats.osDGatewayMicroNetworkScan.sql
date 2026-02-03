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
    `gateway_micro_network_scan`: STRUCT<
      `found_additional_networks`: BOOLEAN,
      `networks`: ARRAY<
        STRUCT<
          `rat`: INT,
          `name`: STRING,
          `mcc_mnc`: STRING,
          `channel_freq`: BIGINT,
          `gsm_stats`: STRUCT<
            `rx_level_dbm`: INT,
            `support_gprs`: BOOLEAN
          >,
          `lte_stats`: STRUCT<
            `rssi_dbm`: INT,
            `rsrp_dbm`: INT,
            `rsrq_db`: INT,
            `physical_cell_id`: BIGINT
          >,
          `is_current_network`: BOOLEAN,
          `lac_tac`: BIGINT,
          `cell_id`: BIGINT,
          `mobile_country_code`: BIGINT,
          `mobile_network_code`: BIGINT,
          `band`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
