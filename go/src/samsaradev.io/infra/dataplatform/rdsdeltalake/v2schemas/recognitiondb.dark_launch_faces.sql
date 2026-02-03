`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`asset_ms` BIGINT,
`face_detail_proto` STRUCT<
  `autoassignment_driver_id`: BIGINT,
  `match_similarity_value`: FLOAT,
  `match_driver_id`: BIGINT,
  `match_device_id`: BIGINT,
  `match_asset_ms`: BIGINT,
  `driver_album_info`: STRUCT<
    `start_ms`: BIGINT,
    `end_ms`: BIGINT
  >
>,
`_raw_face_detail_proto` STRING,
`s3_key` STRING,
`identity_uuid` STRING,
`experiment_tag` SHORT,
`date` STRING
