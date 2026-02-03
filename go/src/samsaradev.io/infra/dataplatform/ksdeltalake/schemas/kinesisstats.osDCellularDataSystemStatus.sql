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
    `cellular_data_system_status`: STRUCT<
      `mdm9607`: STRUCT<
        `status`: STRUCT<
          `primary_system`: STRUCT<
            `network`: INT,
            `rat`: INT,
            `service_option_mask_low_bits`: INT,
            `service_option_mask_high_bits`: INT
          >,
          `secondary_systems`: ARRAY<
            STRUCT<
              `network`: INT,
              `rat`: INT,
              `service_option_mask_low_bits`: INT,
              `service_option_mask_high_bits`: INT
            >
          >,
          `rssi_dbm`: INT,
          `operator`: STRING,
          `sim_slot`: INT,
          `euicc_profile`: STRUCT<
            `profile_name`: STRING,
            `service_provider_name`: STRING
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
