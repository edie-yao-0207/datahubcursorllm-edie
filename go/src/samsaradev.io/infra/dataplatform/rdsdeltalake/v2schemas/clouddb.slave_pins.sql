`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`id` BIGINT,
`slave_device_id` BIGINT,
`name` STRING,
`units` STRING,
`config_proto` STRUCT<
  `function_code`: INT,
  `starting_address`: BIGINT,
  `length`: BIGINT,
  `polling_interval_ms`: BIGINT,
  `scale_factor`: DOUBLE,
  `data_type`: INT,
  `eip_config`: STRUCT<
    `ioi_path`: STRING,
    `elem_type`: INT,
    `elem_count`: BIGINT,
    `tag_name`: STRING,
    `polling_interval_ms`: BIGINT
  >,
  `opc_ua_config`: STRUCT<
    `namespace`: BIGINT,
    `name`: STRING,
    `node_name_type`: INT,
    `node_data_type`: INT
  >,
  `mqtt_config`: STRUCT<`mqtt_topic`: STRING>,
  `options`: STRUCT<
    `word_swap`: BOOLEAN,
    `byte_swap`: BOOLEAN
  >,
  `command`: STRUCT<
    `command`: STRING,
    `response_regex`: STRING,
    `regex_match_index`: BIGINT,
    `regex_submatch_index`: BIGINT
  >,
  `roc_config`: STRUCT<
    `tlp_data_type`: INT,
    `polling_interval_ms`: BIGINT,
    `type`: BIGINT,
    `logical`: BIGINT,
    `parameter`: BIGINT,
    `opcode_config`: STRUCT<`opcode_type`: INT>
  >,
  `spn_config`: STRUCT<
    `spn_id`: BIGINT,
    `preferred_tx_id`: BIGINT
  >,
  `s7_config`: STRUCT<
    `s7_operand_type`: INT,
    `s7_data_type`: INT,
    `address`: BIGINT,
    `length`: BIGINT,
    `bit`: BIGINT,
    `database_settings`: STRUCT<`database_number`: BIGINT>
  >
>,
`_raw_config_proto` STRING,
`created_by` BIGINT,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`slave_device_type` SHORT,
`template_third_party_pin_id` BIGINT,
`partition` STRING
