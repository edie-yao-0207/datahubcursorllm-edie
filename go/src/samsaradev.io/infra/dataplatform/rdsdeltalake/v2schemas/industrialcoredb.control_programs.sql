`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`program_id` BIGINT,
`name` STRING,
`program_type` BYTE,
`code` STRUCT<
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
`_raw_code` STRING,
`deployments` STRING,
`libraries` STRUCT<
  `library_uuids`: ARRAY<BINARY>
>,
`_raw_libraries` STRING,
`created_by` INT,
`created_at` TIMESTAMP,
`updated_by` INT,
`updated_at` TIMESTAMP,
`deleted` BYTE,
`partition` STRING
