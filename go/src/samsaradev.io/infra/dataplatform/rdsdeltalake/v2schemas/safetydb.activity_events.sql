`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`event_ms` BIGINT,
`created_at` TIMESTAMP,
`activity_type` BIGINT,
`detail_proto` STRUCT<
  `user_id`: BIGINT,
  `comment_content`: STRUCT<
    `comment_text`: STRING,
    `url`: STRING
  >,
  `inbox_content`: STRUCT<`new_state`: INT>,
  `manager_assignment_content`: STRUCT<
    `manager_user_id`: BIGINT,
    `prev_manager_user_id`: BIGINT
  >,
  `coaching_content`: STRUCT<
    `new_state`: INT,
    `manager_user_id`: BIGINT,
    `main_event_identifier`: STRUCT<
      `event_ms`: BIGINT,
      `device_id`: BIGINT
    >,
    `reason`: INT
  >,
  `harsh_event_label_content`: STRUCT<`new_user_label`: INT>,
  `behavior_label_content`: STRUCT<
    `label_type`: INT,
    `new_value`: BOOLEAN,
    `algorithm_version`: INT
  >,
  `trigger_label_content`: STRUCT<
    `severity_level`: INT,
    `algorithm_version`: INT,
    `trigger_reason`: INT
  >,
  `driver_comment_content`: STRUCT<`comment_text`: STRING>,
  `driver_assignment_content`: STRUCT<
    `driver_id`: BIGINT,
    `prev_driver_id`: BIGINT
  >,
  `dismissal_with_reason_content`: STRUCT<
    `dismissal_reason`: INT,
    `comment_text`: STRING
  >,
  `triage_label_content`: STRUCT<
    `label_id`: STRING,
    `label_name`: STRING,
    `new_value`: BOOLEAN
  >,
  `user_type`: INT
>,
`_raw_detail_proto` STRING,
`uuid` STRING,
`date` STRING
