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
    `fill_level_change`: STRUCT<
      `delta_mass_ingress_grams`: BIGINT,
      `delta_mass_egress_grams`: BIGINT,
      `delta_volume_ingress_milliliters`: BIGINT,
      `delta_volume_egress_milliliters`: BIGINT,
      `fill_event_detection_metadata`: STRUCT<
        `baseline_algorithm`: STRUCT<
          `current_baseline`: STRUCT<
            `time_ms`: DECIMAL(20, 0),
            `volume_milliliters`: BIGINT,
            `mass_grams`: BIGINT
          >,
          `candidate_baselines`: ARRAY<
            STRUCT<
              `time_ms`: DECIMAL(20, 0),
              `volume_milliliters`: BIGINT,
              `mass_grams`: BIGINT
            >
          >,
          `latest_similar_baseline`: STRUCT<
            `time_ms`: DECIMAL(20, 0),
            `volume_milliliters`: BIGINT,
            `mass_grams`: BIGINT
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
