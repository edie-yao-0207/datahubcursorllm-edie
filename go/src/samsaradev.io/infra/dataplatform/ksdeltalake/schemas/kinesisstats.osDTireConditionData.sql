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
    `tire_condition_per_asset`: STRUCT<
      `asset`: ARRAY<
        STRUCT<
          `type`: INT,
          `manufacturer`: INT,
          `manufacturer_code`: BIGINT,
          `source_address`: BIGINT,
          `tire`: ARRAY<
            STRUCT<
              `tire_from_left`: BIGINT,
              `axle_from_front`: BIGINT,
              `pressure_k_pa`: BIGINT,
              `temperature_milli_c`: INT,
              `sensor_status`: INT,
              `tire_status`: INT,
              `pressure_threshold`: INT,
              `temperature_status`: INT,
              `tire_alerts`: ARRAY<INT>,
              `offset_from_reported_ms`: INT,
              `tire_pressure_reference_info`: STRUCT<
                `reference_pressure_k_pa`: BIGINT,
                `reference_pressure_valid`: BOOLEAN
              >,
              `pressure_high_range_k_pa`: BIGINT,
              `tire_pressure_compensation_info`: STRUCT<
                `compensation_pressure_k_pa`: BIGINT,
                `compensation_temperature_milli_c`: INT
              >,
              `tire_alert_recommendations`: ARRAY<
                STRUCT<
                  `alert_event`: STRING,
                  `recommendation_message`: STRING
                >
              >
            >
          >,
          `serial_number`: STRING
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
