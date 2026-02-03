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
    `cellular_debug`: STRUCT<
      `scanned_networks`: ARRAY<
        STRUCT<
          `mobile_country_code`: BIGINT,
          `mobile_network_code`: BIGINT,
          `three_digit_mnc`: BOOLEAN,
          `radio_access_technology`: INT,
          `in_use_status`: INT,
          `roaming_status`: INT,
          `forbidden_status`: INT,
          `preferred_status`: INT,
          `name_source`: INT,
          `name`: STRING
        >
      >,
      `latitude_nd`: BIGINT,
      `longitude_nd`: BIGINT,
      `speed_horizontal_mmpers`: BIGINT,
      `heading_d`: FLOAT,
      `hub_connection_type`: INT,
      `have_network_scan`: BOOLEAN,
      `serving_system`: STRUCT<
        `registration_status`: INT,
        `roaming_status`: INT,
        `mobile_country_code`: BIGINT,
        `mobile_network_code`: BIGINT,
        `three_digit_mnc`: BOOLEAN,
        `name`: STRING,
        `lac`: BIGINT,
        `cell_id`: BIGINT,
        `tac`: BIGINT,
        `radio_infos`: ARRAY<
          STRUCT<
            `radio_interface`: INT,
            `band_class`: BIGINT,
            `active_channel`: BIGINT
          >
        >,
        `rssi_dbm`: INT,
        `signal_info`: STRUCT<
          `cdma_signal`: STRUCT<
            `rssi_dbm`: INT,
            `ecio_tenth_db`: INT
          >,
          `hdr_signal`: STRUCT<
            `common`: STRUCT<
              `rssi_dbm`: INT,
              `ecio_tenth_db`: INT
            >,
            `sinr_tenth_db`: INT,
            `io_dbm`: INT
          >,
          `gsm_rssi_dbm`: INT,
          `wcdma_signal`: STRUCT<
            `rssi_dbm`: INT,
            `ecio_tenth_db`: INT
          >,
          `lte_signal`: STRUCT<
            `rssi_dbm`: INT,
            `rsrq_tenth_db`: INT,
            `rsrp_dbm`: INT,
            `snr_tenth_db`: INT
          >,
          `tdscdma_signal`: STRUCT<
            `rssi_dbm`: INT,
            `rscp_dbm`: INT,
            `ecio_tenth_db`: INT,
            `sinr_tenth_db`: INT
          >
        >,
        `attached_state`: INT
      >,
      `startup_log`: BOOLEAN,
      `lte_ml1_band_scan_candidates`: ARRAY<
        STRUCT<
          `earfcn`: BIGINT,
          `band`: BIGINT,
          `bandwidth`: INT,
          `energy`: INT
        >
      >,
      `wcdma_freq_scan_samples`: ARRAY<
        STRUCT<
          `scan_type`: INT,
          `wcdma_scan_threshold`: INT,
          `rssi`: INT,
          `arfcn`: BIGINT
        >
      >,
      `gsm_power_scan_frequencies`: ARRAY<
        STRUCT<
          `scan_power_thresh`: INT,
          `abs_rf_chan_num`: BIGINT,
          `band`: INT,
          `recevied_power`: INT
        >
      >,
      `network_scan_band_pref_bitmask`: DECIMAL(20, 0),
      `network_scan_duration_ms`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
