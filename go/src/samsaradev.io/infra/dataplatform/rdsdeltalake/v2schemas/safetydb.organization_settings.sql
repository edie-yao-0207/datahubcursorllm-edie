`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`organization_setting` STRUCT<
  `mobile_usage_settings`: STRUCT<
    `enabled`: BOOLEAN,
    `audio_alerts_enabled`: BOOLEAN,
    `minimum_speed_enum`: INT,
    `severity_threshold_enum`: INT,
    `audio_alert_promotion_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `alert_count_threshold`: BIGINT,
      `duration_ms`: BIGINT,
      `per_trip_enabled`: BOOLEAN
    >
  >,
  `distracted_driving_settings`: STRUCT<
    `enabled`: BOOLEAN,
    `audio_alerts_enabled`: BOOLEAN,
    `minimum_speed_enum`: INT,
    `severity_threshold_enum`: INT,
    `audio_alert_promotion_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `alert_count_threshold`: BIGINT,
      `duration_ms`: BIGINT,
      `per_trip_enabled`: BOOLEAN
    >
  >,
  `following_distance_settings`: STRUCT<
    `enabled`: BOOLEAN,
    `audio_alerts_enabled`: BOOLEAN,
    `minimum_speed_enum`: INT,
    `severity_threshold_enum`: INT,
    `audio_alert_promotion_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `alert_count_threshold`: BIGINT,
      `duration_ms`: BIGINT,
      `per_trip_enabled`: BOOLEAN
    >
  >,
  `eating_drinking_settings`: STRUCT<
    `enabled`: BOOLEAN,
    `audio_alerts_enabled`: BOOLEAN,
    `minimum_speed_enum`: INT,
    `severity_threshold_enum`: INT,
    `audio_alert_promotion_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `alert_count_threshold`: BIGINT,
      `duration_ms`: BIGINT,
      `per_trip_enabled`: BOOLEAN
    >
  >,
  `video_retrieval_settings`: STRUCT<`video_retrievals_disabled`: BOOLEAN>,
  `in_cab_audio_alert_settings`: STRUCT<
    `voiceless_alert_settings`: STRUCT<`enabled`: BOOLEAN>
  >,
  `periodic_still_frequency`: STRUCT<
    `custom_frequency`: INT,
    `periodic_stills_enabled_settings`: STRUCT<
      `outward_stills_enabled`: BOOLEAN,
      `inward_stills_enabled`: BOOLEAN
    >
  >,
  `lane_departure_warning_settings`: STRUCT<
    `enabled`: BOOLEAN,
    `audio_alerts_enabled`: BOOLEAN,
    `minimum_speed_enum`: INT,
    `severity_threshold_enum`: INT,
    `audio_alert_promotion_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `alert_count_threshold`: BIGINT,
      `duration_ms`: BIGINT,
      `per_trip_enabled`: BOOLEAN
    >
  >,
  `recording_settings`: STRUCT<
    `camera_recording_mode_enum`: INT,
    `primary_camera_disabled`: BOOLEAN,
    `driver_facing_camera_disabled`: BOOLEAN,
    `external_camera_disabled`: BOOLEAN
  >,
  `driverless_nudges_settings`: STRUCT<`driverless_nudges_enabled`: BOOLEAN>,
  `ml_mode_settings`: STRUCT<`ml_mode`: INT>,
  `seatbelt_settings`: STRUCT<
    `enabled`: BOOLEAN,
    `audio_alerts_enabled`: BOOLEAN,
    `minimum_speed_enum`: INT,
    `severity_threshold_enum`: INT,
    `audio_alert_promotion_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `alert_count_threshold`: BIGINT,
      `duration_ms`: BIGINT,
      `per_trip_enabled`: BOOLEAN
    >
  >,
  `inward_obstruction_settings`: STRUCT<
    `enabled`: BOOLEAN,
    `audio_alerts_enabled`: BOOLEAN,
    `minimum_speed_enum`: INT,
    `severity_threshold_enum`: INT,
    `audio_alert_promotion_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `alert_count_threshold`: BIGINT,
      `duration_ms`: BIGINT,
      `per_trip_enabled`: BOOLEAN
    >
  >,
  `limit_nudge_event_upload_settings`: STRUCT<`limit_nudge_event_upload_enabled`: BOOLEAN>,
  `sensitive_media_settings`: STRUCT<`crash_events_enabled`: BOOLEAN>
>,
`_raw_organization_setting` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`partition` STRING
