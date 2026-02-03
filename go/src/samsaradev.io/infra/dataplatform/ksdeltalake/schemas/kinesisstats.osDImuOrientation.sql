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
    `imu_orientation`: STRUCT<
      `gravity_vector`: STRUCT<
        `x`: FLOAT,
        `y`: FLOAT,
        `z`: FLOAT
      >,
      `yaw_angle_degrees`: FLOAT,
      `calibration_index`: BIGINT,
      `rotation_quaternion`: STRUCT<
        `w`: FLOAT,
        `v`: STRUCT<
          `x`: FLOAT,
          `y`: FLOAT,
          `z`: FLOAT
        >
      >,
      `is_loaded_from_disk`: BOOLEAN,
      `lock_valid`: INT,
      `algorithm_version`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
