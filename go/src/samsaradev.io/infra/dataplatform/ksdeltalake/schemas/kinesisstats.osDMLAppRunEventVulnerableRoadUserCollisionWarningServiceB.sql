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
    `mlapp_run_event`: STRUCT<
      `run_tag`: STRUCT<`run_id`: BIGINT>,
      `run_event_type`: INT,
      `id`: STRUCT<`name`: STRING>,
      `model_file_state`: STRUCT<
        `dlcs`: ARRAY<
          STRUCT<
            `name`: STRING,
            `version`: STRING
          >
        >,
        `model_registry_key`: STRING
      >,
      `run_period`: INT,
      `start_reason`: INT,
      `stop_reason`: INT,
      `pipeline_runtime`: INT,
      `profile_params`: STRUCT<
        `profile_name`: STRING,
        `cohort_name`: STRING,
        `feature_name`: STRING
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
