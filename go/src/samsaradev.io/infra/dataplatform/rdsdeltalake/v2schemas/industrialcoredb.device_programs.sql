`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`program_id` BIGINT,
`program_uuid` STRING,
`program_hash` STRING,
`deployment` STRUCT<
  `aliases`: ARRAY<
    STRUCT<
      `name`: STRING,
      `type`: INT,
      `data_input`: STRUCT<`id`: BIGINT>,
      `data_output`: STRUCT<`uuid`: BINARY>,
      `local_variable`: STRUCT<`id`: BIGINT>,
      `gateway_io`: STRUCT<
        `pin_number`: BIGINT,
        `module_id`: BIGINT,
        `type`: INT
      >
    >
  >,
  `program_variables`: STRUCT<
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
  `scan_interval_ms`: BIGINT,
  `text_files`: ARRAY<STRING>,
  `ladder_template`: STRUCT<
    `rungs`: ARRAY<
      STRUCT<
        `nodes`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `type`: INT,
            `next_nodes`: ARRAY<BIGINT>,
            `paired_junction`: BIGINT
          >
        >,
        `comment`: STRING,
        `title`: STRING
      >
    >,
    `components`: ARRAY<
      STRUCT<
        `id`: BIGINT,
        `type`: INT,
        `config`: STRUCT<
          `parameters`: ARRAY<
            STRUCT<
              `name`: STRING,
              `alias`: STRING
            >
          >,
          `options`: STRUCT<
            `function_block_reference`: STRING,
            `negated`: BOOLEAN,
            `coil_latched`: BOOLEAN,
            `comparator`: INT,
            `expression`: STRING
          >
        >,
        `instance_name`: STRING
      >
    >
  >
>,
`_raw_deployment` STRING,
`deployed` BYTE,
`deployed_at` TIMESTAMP,
`ended_at` TIMESTAMP,
`uuid` STRING,
`partition` STRING
