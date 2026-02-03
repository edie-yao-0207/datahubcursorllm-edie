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
    `bandwidth_report`: STRUCT<
      `lte_category`: INT,
      `instance_type`: STRING,
      `upload_bytes`: BIGINT,
      `download_bytes`: BIGINT,
      `upload_duration_ms`: BIGINT,
      `latitude_nd`: BIGINT,
      `longitude_nd`: BIGINT,
      `download_duration_ms`: BIGINT,
      `send_buffer_bytes`: BIGINT,
      `receive_buffer_bytes`: BIGINT,
      `server`: STRING,
      `avg_rtt_us`: BIGINT,
      `min_rtt_us`: BIGINT,
      `max_rtt_us`: BIGINT,
      `cellular_sim_slot`: INT,
      `cellular_rat`: INT,
      `cellular_operator`: STRING,
      `cellular_rssi_dbm`: INT,
      `euicc_profile`: STRUCT<
        `profile_name`: STRING,
        `service_provider_name`: STRING
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
