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
    `octo_connected_camera_video_info`: STRUCT<
      `bitrate`: BIGINT,
      `framerate`: BIGINT,
      `codec`: INT,
      `resolution`: INT,
      `bitrate_control`: INT,
      `idr_interval_seconds`: INT,
      `video_transform`: STRUCT<
        `flip_vertical`: BOOLEAN,
        `flip_horizontal`: BOOLEAN,
        `rotation`: INT
      >,
      `bframes_m_value`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
