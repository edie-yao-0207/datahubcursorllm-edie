`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`program_id` BIGINT,
`active` BYTE,
`structured_text` STRING,
`checksum` STRING,
`created_at` TIMESTAMP,
`created_by` INT,
`updated_at` TIMESTAMP,
`updated_by` INT,
`name` STRING,
`program_variables` STRUCT<
  `variables`: ARRAY<
    STRUCT<
      `name`: STRING,
      `machineInputId`: BIGINT,
      `machine_output_uuid`: STRING,
      `local_variable_id`: BIGINT,
      `gateway_io`: STRUCT<
        `pin_number`: BIGINT,
        `module_id`: BIGINT,
        `module_gateway_id`: BIGINT
      >,
      `type`: INT,
      `metadata`: STRUCT<
        `statType`: BIGINT,
        `objectType`: BIGINT,
        `objectId`: BIGINT,
        `formula`: STRING
      >,
      `variable_type`: BIGINT
    >
  >
>,
`_raw_program_variables` STRING,
`program_hash` STRING,
`partition` STRING
