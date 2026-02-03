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
    `ebs_load_plate_info`: STRUCT<
      `axle_load`: STRUCT<
        `num_axles_reported`: BIGINT,
        `axle_load_io`: ARRAY<
          STRUCT<
            `unladen_pressure_millibar`: BIGINT,
            `laden_pressure_millibar`: BIGINT,
            `unladen_axle_weight_kg`: BIGINT,
            `laden_axle_weight_kg`: BIGINT
          >
        >
      >,
      `brake_press_io`: ARRAY<
        STRUCT<
          `demand_press_millibar`: BIGINT,
          `laden_output_press_millibar`: BIGINT,
          `unladen_press_defined`: BOOLEAN,
          `unladen_output_press_millibar`: BIGINT
        >
      >,
      `max_input_pressure_millibar`: BIGINT,
      `onset_pressure_millibar`: BIGINT,
      `brake_calc_number`: STRING,
      `trailer_model`: STRING,
      `chassis_number`: STRING,
      `trailer_manufacturer`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
