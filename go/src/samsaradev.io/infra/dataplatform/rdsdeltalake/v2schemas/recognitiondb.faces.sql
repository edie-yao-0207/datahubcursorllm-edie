`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`asset_ms` BIGINT,
`s3_key` STRING,
`face_detail_proto` STRUCT<
  `bounding_box`: STRUCT<
    `top`: DOUBLE,
    `left`: DOUBLE,
    `height`: DOUBLE,
    `width`: DOUBLE
  >,
  `autoassignment_driver_id`: BIGINT,
  `match_similarity_value`: FLOAT,
  `match_device_id`: BIGINT,
  `match_asset_ms`: BIGINT,
  `driver_album_info`: STRUCT<
    `start_ms`: BIGINT,
    `end_ms`: BIGINT
  >,
  `v3_rekognition_id_backup`: STRING,
  `v7_rekognition_id_backup`: STRING
>,
`_raw_face_detail_proto` STRING,
`rekognition_id` STRING,
`assignment_source` BYTE,
`updated_at_ms` BIGINT,
`driver_id` BIGINT,
`autoassigned_driver_id` BIGINT,
`assigned_by` BIGINT,
`assigned_at_ms` BIGINT,
`autoassignment_group_uuid` STRING,
`date` STRING
