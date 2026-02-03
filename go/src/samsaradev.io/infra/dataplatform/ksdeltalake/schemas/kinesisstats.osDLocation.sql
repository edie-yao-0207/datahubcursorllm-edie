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
    `asset_location`: STRUCT<
      `latitude_microdegrees`: BIGINT,
      `longitude_microdegrees`: BIGINT,
      `horizontal_uncertainty_decimeters`: BIGINT,
      `gnss_speed_centimeters_per_second`: BIGINT,
      `gnss_speed_uncertainty_centimeters_per_second`: BIGINT,
      `ecu_speed_centimeters_per_second`: BIGINT,
      `altitude_and_heading`: STRUCT<
        `mean_sea_level_altitude_decimeters`: INT,
        `altitude_uncertainty_decimeters`: BIGINT,
        `mean_sea_level_to_wsg84_delta_meters`: INT,
        `heading_degrees`: BIGINT,
        `heading_uncertainty_degrees`: BIGINT
      >,
      `way_id`: DECIMAL(20, 0),
      `speed_limit_kmph`: FLOAT,
      `delta_locations`: STRUCT<
        `num_locations`: BIGINT,
        `presence_and_validity_bitmap`: ARRAY<BIGINT>,
        `delta_latitude_microdegrees`: ARRAY<INT>,
        `delta_longitude_microdegrees`: ARRAY<INT>,
        `delta_gnss_speed_centimeters_per_second`: ARRAY<INT>,
        `delta_ecu_speed_centimeters_per_second`: ARRAY<INT>,
        `delta_heading_degrees`: ARRAY<INT>,
        `delta_second_tenths`: ARRAY<BIGINT>,
        `max_horizontal_uncertainty_meters`: BIGINT,
        `max_speed_uncertainty_decimeters_per_second`: BIGINT,
        `max_heading_uncertainty_degrees`: BIGINT
      >,
      `invalid_fields_bitmap`: BIGINT,
      `external_antenna`: BOOLEAN,
      `is_spoofed`: BOOLEAN,
      `is_fixed`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
