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
    `interface_state`: STRUCT<
      `primary_uplink`: INT,
      `wifi`: STRUCT<
        `uplink`: STRUCT<
          `up`: BOOLEAN,
          `mac`: STRING,
          `ip`: STRING,
          `subnet`: STRING,
          `router`: STRING,
          `dns1`: STRING,
          `dns2`: STRING
        >,
        `ssid`: STRING,
        `rssi_dbm`: INT
      >,
      `cellular`: STRUCT<
        `uplink`: STRUCT<
          `up`: BOOLEAN,
          `mac`: STRING,
          `ip`: STRING,
          `subnet`: STRING,
          `router`: STRING,
          `dns1`: STRING,
          `dns2`: STRING
        >,
        `imei`: STRING,
        `rssi_dbm`: INT,
        `ber`: INT,
        `operator_s`: STRING,
        `iccid`: STRING,
        `rsrp_db`: INT,
        `sinr_db`: INT,
        `rsrq_db`: INT,
        `sim_slot`: INT,
        `apn_used`: STRING,
        `euicc_profile`: STRUCT<
          `profile_name`: STRING,
          `service_provider_name`: STRING
        >
      >,
      `update_type`: INT,
      `ethernet`: STRUCT<
        `uplink`: STRUCT<
          `up`: BOOLEAN,
          `mac`: STRING,
          `ip`: STRING,
          `subnet`: STRING,
          `router`: STRING,
          `dns1`: STRING,
          `dns2`: STRING
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
