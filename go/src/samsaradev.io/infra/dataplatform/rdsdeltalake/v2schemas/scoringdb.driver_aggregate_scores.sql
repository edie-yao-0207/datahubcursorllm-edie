`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`agg_window` INT,
`driver_id` BIGINT,
`start_ms` BIGINT,
`end_ms` BIGINT,
`trip_data` STRUCT<
  `distance_meters`: BIGINT,
  `duration_ms`: BIGINT,
  `speeding_durations`: STRUCT<
    `light_ms`: BIGINT,
    `moderate_ms`: BIGINT,
    `heavy_ms`: BIGINT,
    `severe_ms`: BIGINT
  >,
  `kph_speeding_durations`: STRUCT<
    `light_ms`: BIGINT,
    `moderate_ms`: BIGINT,
    `heavy_ms`: BIGINT,
    `severe_ms`: BIGINT
  >,
  `percent_speeding_durations`: STRUCT<
    `light_ms`: BIGINT,
    `moderate_ms`: BIGINT,
    `heavy_ms`: BIGINT,
    `severe_ms`: BIGINT
  >,
  `seatbelt_unbuckled_ms`: BIGINT,
  `seatbelt_reporting_present`: BOOLEAN,
  `max_speed_at_ms`: BIGINT,
  `max_speed_kmph`: DOUBLE
>,
`_raw_trip_data` STRING,
`config_snapshot` STRUCT<
  `enabled_cameras`: ARRAY<INT>,
  `enabled_features`: ARRAY<INT>,
  `aggregated_cameras`: ARRAY<
    STRUCT<
      `camera`: INT,
      `distance_meters`: BIGINT,
      `duration_ms`: BIGINT
    >
  >
>,
`_raw_config_snapshot` STRING,
`triage_events` STRUCT<
  `event_data`: ARRAY<
    STRUCT<
      `behaviors`: ARRAY<
        STRUCT<
          `behavior_label`: INT,
          `detection_method`: INT
        >
      >,
      `context_labels`: ARRAY<
        STRUCT<
          `label_id`: STRING,
          `detection_method`: INT
        >
      >,
      `start_ms`: BIGINT,
      `end_ms`: BIGINT,
      `trip_start_ms`: BIGINT,
      `triage_state`: INT
    >
  >,
  `overflow`: BOOLEAN
>,
`_raw_triage_events` STRING,
`date` STRING
