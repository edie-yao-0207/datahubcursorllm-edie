`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`name` STRING,
`uri` STRING,
`enable_motion_detection` BYTE,
`org_id` BIGINT,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted_at` TIMESTAMP,
`camera_device_id` BIGINT,
`extra_info` STRUCT<
  `extra_rtsp_channels`: ARRAY<
    STRUCT<
      `channel`: INT,
      `uri`: STRING,
      `created_at_ms`: BIGINT,
      `store_to_disk`: INT,
      `external_channel_id`: STRUCT<`vivotek_id`: BIGINT>
    >
  >,
  `external_stream_id`: STRUCT<`vivotek_id`: BIGINT>,
  `primary_external_channel_id`: STRUCT<`vivotek_id`: BIGINT>
>,
`_raw_extra_info` STRING,
`motion_threshold_magnitude` INT,
`motion_threshold_duration_ms` BIGINT,
`allow_auto_configuration` BYTE,
`primary_channel_settings_updated_at` TIMESTAMP,
`partition` STRING
