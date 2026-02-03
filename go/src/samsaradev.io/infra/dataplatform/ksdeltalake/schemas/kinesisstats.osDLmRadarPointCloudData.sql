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
    `lm_radar_point_cloud_data`: STRUCT<
      `points`: ARRAY<
        STRUCT<
          `distance_mm`: BIGINT,
          `snr`: BIGINT,
          `noise`: BIGINT,
          `sequence_id`: BIGINT,
          `accel_data`: STRUCT<
            `x_ug`: ARRAY<INT>,
            `y_ug`: ARRAY<INT>,
            `z_ug`: ARRAY<INT>,
            `num_samples`: BIGINT,
            `delta_encoded`: BOOLEAN,
            `in_motion`: BOOLEAN
          >,
          `measurement_status`: INT,
          `measurement_skipped`: BOOLEAN
        >
      >,
      `sequence_id`: BIGINT,
      `point_cloud_offset_idx`: BIGINT,
      `temperature_deci_c`: INT,
      `obstruction_value`: BIGINT,
      `calibrated_offset_mm`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
