`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`organization_setting` STRUCT<
  `coaching_due_date_duration_ms`: DECIMAL(20, 0),
  `safety_labels_to_automatically_share_with_driver`: ARRAY<INT>,
  `safety_labels_to_automatically_share_with_driver_for_coaching`: ARRAY<INT>,
  `self_coaching_settings`: STRUCT<
    `driver_self_coaching_push_notifications_enabled`: BOOLEAN,
    `driver_self_coaching_driver_review_enabled`: BOOLEAN,
    `driver_self_coaching_manager_required`: BOOLEAN,
    `driver_self_coaching_notification`: INT,
    `driver_self_coaching_hide_from_manager`: BOOLEAN,
    `driver_self_coaching_disputes_enabled`: BOOLEAN,
    `driver_self_coaching_disputes_excluded_driver_ids`: ARRAY<BIGINT>,
    `driver_self_coaching_disputes_manager_notifications_enabled`: BOOLEAN
  >
>,
`_raw_organization_setting` STRING,
`updated_at` TIMESTAMP,
`date` STRING
