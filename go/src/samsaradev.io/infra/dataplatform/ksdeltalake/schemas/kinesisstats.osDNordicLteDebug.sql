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
    `nordic_lte_debug`: STRUCT<
      `lte_cell_info`: STRUCT<
        `mcc`: BIGINT,
        `mnc`: BIGINT,
        `cell_id`: BIGINT,
        `earfcn`: BIGINT,
        `tac`: BIGINT,
        `physical_cell_id`: BIGINT,
        `rsrp_dbm`: INT,
        `rsrq_db`: INT,
        `plmn_mccmnc`: STRING,
        `operator_short`: STRING,
        `periodic_tau_s`: BIGINT,
        `access_technology`: INT,
        `snr_db`: BIGINT,
        `energy_estimate`: BIGINT,
        `signal_to_noise_ratio_db`: INT
      >,
      `system_state`: INT,
      `modem_fw_version`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
