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
    `dead_reckoning_debug`: STRUCT<
      `dr_latitude_nanodegrees`: BIGINT,
      `dr_longitude_nanodegrees`: BIGINT,
      `dr_accuracy_millimeters`: BIGINT,
      `dr_speed_milliknots`: BIGINT,
      `dr_heading_millidegrees`: BIGINT,
      `dr_utc_ms`: DECIMAL(20, 0),
      `gnss_latitude_nanodegrees`: BIGINT,
      `gnss_longitude_nanodegrees`: BIGINT,
      `gnss_accuracy_millimeters`: BIGINT,
      `gnss_speed_milliknots`: BIGINT,
      `gnss_heading_millidegrees`: BIGINT,
      `gnss_utc_ms`: DECIMAL(20, 0),
      `dr_calibration_percent`: BIGINT,
      `dr_calibration_status_mask`: INT,
      `dr_engine_output_mask`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
