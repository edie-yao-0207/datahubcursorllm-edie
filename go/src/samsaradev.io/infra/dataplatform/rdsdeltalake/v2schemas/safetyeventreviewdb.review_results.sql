`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`review_request_metadata_uuid` STRING,
`job_uuid` STRING,
`source_id` STRING,
`item_type` SHORT,
`review_type` SHORT,
`reviewer_id` BIGINT,
`result_type` SHORT,
`detail_proto` STRUCT<
  `safety_event_result_detail`: STRUCT<
    `behavior_labels`: ARRAY<INT>,
    `initial_trigger_label`: INT,
    `result_type`: INT,
    `reviewed_by_qa_reviewer`: BOOLEAN,
    `context_label_ids`: ARRAY<STRING>,
    `not_selected_context_label_ids`: ARRAY<STRING>
  >
>,
`_raw_detail_proto` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`date` STRING
