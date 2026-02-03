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
    `ai_two_way_agent_call_triggered`: STRUCT<
      `agent_run_id`: STRING,
      `backend_run_id`: STRING,
      `happy_robot_workflow_parameters`: STRUCT<
        `api_host`: STRING,
        `happy_robot_org_id`: STRING,
        `use_case_id`: STRING
      >,
      `trigger_call_reason`: INT,
      `trigger_call_status`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
