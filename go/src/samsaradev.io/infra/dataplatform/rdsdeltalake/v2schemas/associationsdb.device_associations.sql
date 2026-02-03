`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`group_id` BIGINT,
`tractor_id` BIGINT,
`trailer_id` BIGINT,
`start_ms` BIGINT,
`end_ms` BIGINT,
`version` INT,
`hook_metadata` STRUCT<
  `thresholds`: STRUCT<
    `association_confidence_threshold`: DOUBLE,
    `high_pass_filter_confidence_threshold`: DOUBLE,
    `distance_associated_meters`: BIGINT,
    `distance_unassociated_meters`: BIGINT,
    `distance_heuristic_max_confidence`: DOUBLE,
    `time_associated_ms`: BIGINT,
    `time_unassociated_ms`: BIGINT,
    `time_heuristic_maximum_confidence`: DOUBLE
  >,
  `for_confidence`: DOUBLE,
  `against_confidence`: DOUBLE,
  `distance_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `trip_start_point_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `trip_start_time_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `trip_end_point_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `trip_existence_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `ongoing_trip_endpoint_flag`: INT,
  `location_path_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >
>,
`_raw_hook_metadata` STRING,
`drop_metadata` STRUCT<
  `thresholds`: STRUCT<
    `association_confidence_threshold`: DOUBLE,
    `high_pass_filter_confidence_threshold`: DOUBLE,
    `distance_associated_meters`: BIGINT,
    `distance_unassociated_meters`: BIGINT,
    `distance_heuristic_max_confidence`: DOUBLE,
    `time_associated_ms`: BIGINT,
    `time_unassociated_ms`: BIGINT,
    `time_heuristic_maximum_confidence`: DOUBLE
  >,
  `for_confidence`: DOUBLE,
  `against_confidence`: DOUBLE,
  `distance_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `trip_start_point_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `trip_start_time_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `trip_end_point_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `trip_existence_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >,
  `ongoing_trip_endpoint_flag`: INT,
  `location_path_heuristic`: STRUCT<
    `valid`: BOOLEAN,
    `assessment`: BOOLEAN,
    `confidence`: DOUBLE,
    `proto_value`: STRUCT<
      `trip_existence`: STRUCT<`value`: INT>,
      `distance`: STRUCT<
        `distance_meters`: DOUBLE,
        `tractor_reported_at_ms`: BIGINT,
        `trailer_reported_at_ms`: BIGINT,
        `heading_difference_degrees`: DOUBLE,
        `gps_speed_difference_meters_per_second`: DOUBLE
      >,
      `trip_start_point`: STRUCT<
        `distance_meters`: DOUBLE,
        `trailer_trip_distance_meters`: DOUBLE
      >,
      `trip_end_point`: STRUCT<`distance_meters`: DOUBLE>,
      `trip_start_time`: STRUCT<
        `time_difference_ms`: BIGINT,
        `tractor_trip_start_time_ms`: BIGINT,
        `trailer_trip_start_time_ms`: BIGINT
      >,
      `locations_path`: STRUCT<`max_seperation_distance_meters`: DOUBLE>
    >
  >
>,
`_raw_drop_metadata` STRING,
`date` STRING
