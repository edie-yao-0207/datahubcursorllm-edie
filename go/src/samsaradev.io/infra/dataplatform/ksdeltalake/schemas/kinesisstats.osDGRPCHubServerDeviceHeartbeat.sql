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
    `grpc_hub_server_device_heartbeat`: STRUCT<
      `connection`: STRUCT<
        `hubserver_task_arn`: STRING,
        `hubserver_management_address`: STRING,
        `device_hello_request_id`: BIGINT,
        `device_hello`: STRUCT<
          `device_id`: BIGINT,
          `boot_count`: INT,
          `build`: STRING,
          `cfg_ver`: BIGINT,
          `interface`: INT,
          `cell_operator`: STRING,
          `org_id`: BIGINT,
          `group_id`: BIGINT,
          `gateway_id`: BIGINT,
          `desired_keepalive_seconds`: BIGINT,
          `supports_serialized_hub_req`: BOOLEAN,
          `compression`: STRUCT<
            `compression_scheme`: INT,
            `dictionary_sha256`: BINARY
          >,
          `latest_param_overrides_version`: DECIMAL(20, 0),
          `supports_backend_initiated_commands`: BOOLEAN
        >,
        `started_at_ms`: BIGINT,
        `public_ip`: STRING,
        `public_ip_hostname`: STRING,
        `client`: INT,
        `hubserver_grpc_address`: STRING
      >,
      `last_heartbeat_at_ms`: BIGINT,
      `heartbeat_period_sec`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
