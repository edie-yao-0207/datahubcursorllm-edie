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
    `dashcam_state`: STRUCT<
      `camera_state`: INT,
      `recording_reason`: ARRAY<INT>,
      `recording_status`: STRUCT<
        `primary`: STRUCT<
          `high_res`: INT,
          `low_res`: INT,
          `audio`: INT
        >,
        `secondary`: STRUCT<
          `high_res`: INT,
          `low_res`: INT,
          `audio`: INT
        >,
        `analog_1`: STRUCT<
          `high_res`: INT,
          `low_res`: INT,
          `audio`: INT
        >,
        `analog_2`: STRUCT<
          `high_res`: INT,
          `low_res`: INT,
          `audio`: INT
        >,
        `analog_3`: STRUCT<
          `high_res`: INT,
          `low_res`: INT,
          `audio`: INT
        >,
        `analog_4`: STRUCT<
          `high_res`: INT,
          `low_res`: INT,
          `audio`: INT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
