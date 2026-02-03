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
    `ble_association_start`: STRUCT<
      `relationship_uuid`: STRING,
      `central_asset_id`: BIGINT,
      `start_ms`: BIGINT,
      `relationship_type`: STRING,
      `peripheral_location`: STRUCT<
        `latitude`: DOUBLE,
        `longitude`: DOUBLE,
        `time_ms`: BIGINT
      >,
      `observations_by_central`: BIGINT,
      `peripheral_distance_moved_with_central_meters`: DOUBLE,
      `total_peripheral_distance_moved_meters`: DOUBLE,
      `processing_window_start_ms`: BIGINT,
      `processing_window_end_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
