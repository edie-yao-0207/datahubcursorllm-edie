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
    `sim_data`: STRUCT<
      `sim_data_entries`: ARRAY<
        STRUCT<
          `sim_slot`: INT,
          `iccid`: STRING,
          `service_provider_name`: STRING,
          `euicc_data`: STRUCT<
            `eid`: STRING,
            `profiles`: ARRAY<
              STRUCT<
                `iccid`: STRING,
                `profile_name`: STRING,
                `service_provider_name`: STRING,
                `class`: INT
              >
            >
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
