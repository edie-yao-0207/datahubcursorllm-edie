`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`id` BIGINT,
`master_device_id` BIGINT,
`slave_device_type` SHORT,
`name` STRING,
`notes` STRING,
`connection_parameters_proto` STRUCT<
  `modbus_slave_id`: BIGINT,
  `serial_port_config`: STRUCT<
    `baud_rate`: INT,
    `data_bits`: INT,
    `stop_bits`: INT,
    `parity`: INT,
    `set_rs485_bias`: BOOLEAN,
    `rs485_termination_enabled`: BOOLEAN,
    `protocol`: INT,
    `port`: INT
  >,
  `hostname`: STRING,
  `port`: BIGINT,
  `eip_config`: STRUCT<
    `ip_address`: STRING,
    `cpu_type`: INT
  >,
  `opc_ua_config`: STRUCT<
    `uri`: STRING,
    `auth_method`: INT
  >,
  `mqtt_config`: STRUCT<
    `uris`: ARRAY<STRING>,
    `auth_type`: INT,
    `username`: STRING,
    `password`: STRING
  >,
  `protocol`: INT,
  `roc_config`: STRUCT<
    `roc_type`: INT,
    `hostname`: STRING,
    `port`: BIGINT,
    `controller_unit`: BIGINT,
    `controller_group`: BIGINT,
    `host_unit`: BIGINT,
    `host_group`: BIGINT,
    `time_config`: STRUCT<`timezone`: STRING>
  >,
  `canbus_config`: STRUCT<
    `dtc_data_input_name`: STRING,
    `dtc_data_input_id`: BIGINT
  >,
  `s7_config`: STRUCT<
    `hostname`: STRING,
    `rack`: BIGINT,
    `slot`: BIGINT
  >
>,
`_raw_connection_parameters_proto` STRING,
`created_by` BIGINT,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`is_template` BYTE,
`template_third_party_device_id` BIGINT,
`partition` STRING
