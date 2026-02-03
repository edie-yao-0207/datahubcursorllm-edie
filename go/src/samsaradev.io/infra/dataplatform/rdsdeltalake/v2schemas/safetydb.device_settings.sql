`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`device_setting` STRUCT<
  `mobile_usage_settings`: STRUCT<
    `enabled`: INT,
    `audio_alerts_enabled`: INT,
    `audio_alert_promotion_settings`: STRUCT<`enabled`: INT>,
    `minimum_speed_settings`: STRUCT<`minimum_speed_enum`: INT>
  >,
  `distracted_driving_settings`: STRUCT<
    `audio_alert_promotion_settings`: STRUCT<`enabled`: INT>
  >,
  `following_distance_settings`: STRUCT<
    `audio_alert_promotion_settings`: STRUCT<`enabled`: INT>
  >,
  `eating_drinking_settings`: STRUCT<
    `audio_alert_promotion_settings`: STRUCT<`enabled`: INT>
  >,
  `lane_departure_warning_settings`: STRUCT<
    `audio_alert_promotion_settings`: STRUCT<`enabled`: INT>,
    `enabled`: INT,
    `audio_alerts_enabled`: INT
  >,
  `ml_mode_settings`: STRUCT<`ml_mode`: INT>,
  `seatbelt_settings`: STRUCT<
    `audio_alert_promotion_settings`: STRUCT<`enabled`: INT>
  >,
  `inward_obstruction_settings`: STRUCT<
    `audio_alert_promotion_settings`: STRUCT<`enabled`: INT>
  >,
  `recording_settings`: STRUCT<
    `primary_camera`: INT,
    `driver_facing_camera`: INT,
    `external_camera`: INT
  >
>,
`_raw_device_setting` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`partition` STRING
