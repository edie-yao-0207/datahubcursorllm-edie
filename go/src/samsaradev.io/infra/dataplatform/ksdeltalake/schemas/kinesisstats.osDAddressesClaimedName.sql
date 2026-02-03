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
    `bus_and_addresses_claimed_names`: ARRAY<
      STRUCT<
        `vehicle_diagnostic_bus`: INT,
        `addresses_claimed_name_info`: ARRAY<
          STRUCT<
            `raw_payload`: DECIMAL(20, 0),
            `ecu_id`: BIGINT,
            `identity_number`: BIGINT,
            `manufacturer_code`: BIGINT,
            `ecu_instance`: BIGINT,
            `function_instance`: BIGINT,
            `function`: BIGINT,
            `vehicle_system`: BIGINT,
            `vehicle_system_instance`: BIGINT,
            `industry_group`: BIGINT,
            `arbitrary_address_capable`: BOOLEAN
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
