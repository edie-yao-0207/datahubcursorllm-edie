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
    `j1939_d1_statuses`: ARRAY<
      STRUCT<
        `tx_id`: INT,
        `mil_status`: INT,
        `red_lamp_status`: INT,
        `amber_lamp_status`: INT,
        `protect_lamp_status`: INT,
        `spn`: INT,
        `fmi`: INT,
        `occurance_count`: INT,
        `backend_only_j1939_fault_data`: STRUCT<
          `fmi_description`: STRING,
          `dtc_description`: STRING,
          `volvo_repair_instructions_url`: STRING
        >,
        `fault_protocol_source`: INT,
        `deprecated_spn_version`: BOOLEAN
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
