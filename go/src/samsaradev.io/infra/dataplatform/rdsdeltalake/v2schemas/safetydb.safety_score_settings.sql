`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`created_at` BIGINT,
`author_id` BIGINT,
`detail_proto` STRUCT<
  `configurable_score_on`: BOOLEAN,
  `custom_settings`: STRUCT<
    `contributions`: STRUCT<
      `harsh_event_contribution_percent`: BIGINT,
      `speeding_contribution_percent`: BIGINT
    >,
    `harsh_event_weights`: STRUCT<
      `acceleration_weight`: BIGINT,
      `braking_weight`: BIGINT,
      `harsh_turn_weight`: BIGINT,
      `crash_weight`: BIGINT,
      `rolling_stop_weight`: BIGINT
    >,
    `behavior_label_weights`: STRUCT<
      `generic_distraction_weight`: BIGINT,
      `drowsy_weight`: BIGINT,
      `late_response_weight`: BIGINT,
      `mobile_usage_weight`: BIGINT,
      `no_seat_belt_weight`: BIGINT,
      `did_not_yield_weight`: BIGINT,
      `near_collision_weight`: BIGINT,
      `ran_red_light_weight`: BIGINT,
      `speeding_weight`: BIGINT,
      `following_distance_severe_weight`: BIGINT,
      `following_distance_moderate_weight`: BIGINT,
      `generic_tailgating_weight`: BIGINT,
      `defensive_driving_weight`: BIGINT,
      `obstructed_camera_weight`: BIGINT,
      `lane_departure_weight`: BIGINT,
      `eating_drinking_weight`: BIGINT,
      `smoking_weight`: BIGINT,
      `no_mask_weight`: BIGINT
    >,
    `speeding_weights`: STRUCT<
      `light_speeding_weight`: BIGINT,
      `moderate_speeding_weight`: BIGINT,
      `heavy_speeding_weight`: BIGINT,
      `severe_speeding_weight`: BIGINT
    >,
    `adas_behavior_weights`: STRUCT<
      `following_distance_weight`: BIGINT,
      `distracted_driving_weight`: BIGINT
    >,
    `org_score_goal`: STRUCT<`org_safety_score_goal`: BIGINT>
  >,
  `harsh_acceleration_weight`: DOUBLE,
  `harsh_braking_weight`: DOUBLE,
  `harsh_turn_weight`: DOUBLE,
  `crash_weight`: DOUBLE,
  `harsh_rolling_stop_weight`: DOUBLE,
  `behavior_generic_distraction_weight`: DOUBLE,
  `behavior_drowsy_weight`: DOUBLE,
  `behavior_late_response_weight`: DOUBLE,
  `behavior_mobile_usage_weight`: DOUBLE,
  `behavior_no_seat_belt_weight`: DOUBLE,
  `behavior_did_not_yield_weight`: DOUBLE,
  `behavior_near_collision_weight`: DOUBLE,
  `behavior_ran_red_light_weight`: DOUBLE,
  `behavior_speeding_weight`: DOUBLE,
  `behavior_following_distance_severe_weight`: DOUBLE,
  `behavior_following_distance_moderate_weight`: DOUBLE,
  `behavior_generic_tailgating_weight`: DOUBLE,
  `behavior_defensive_driving_weight`: DOUBLE,
  `behavior_obstructed_camera_weight`: DOUBLE,
  `behavior_lane_departure_weight`: DOUBLE,
  `behavior_eating_drinking_weight`: DOUBLE,
  `behavior_smoking_weight`: DOUBLE,
  `behavior_no_mask_weight`: DOUBLE,
  `speeding_light_speeding_weight`: DOUBLE,
  `speeding_moderate_speeding_weight`: DOUBLE,
  `speeding_heavy_speeding_weight`: DOUBLE,
  `speeding_severe_speeding_weight`: DOUBLE,
  `adas_following_distance_weight`: DOUBLE,
  `adas_distracted_driving_weight`: DOUBLE,
  `behavior_forward_collision_warning_weight`: DOUBLE,
  `speeding_max_speed_weight`: DOUBLE,
  `vulnerable_road_user_weight`: DOUBLE,
  `org_safety_score_goal`: BIGINT,
  `behavior_weights`: STRUCT<
    `harsh_acceleration`: DECIMAL(20, 0),
    `harsh_braking`: DECIMAL(20, 0),
    `harsh_turn`: DECIMAL(20, 0),
    `crash`: DECIMAL(20, 0),
    `harsh_rolling_stop`: DECIMAL(20, 0),
    `behavior_vulnerable_road_user`: DECIMAL(20, 0),
    `behavior_generic_distraction`: DECIMAL(20, 0),
    `behavior_drowsy`: DECIMAL(20, 0),
    `behavior_late_response`: DECIMAL(20, 0),
    `behavior_mobile_usage`: DECIMAL(20, 0),
    `behavior_no_seat_belt`: DECIMAL(20, 0),
    `behavior_did_not_yield`: DECIMAL(20, 0),
    `behavior_near_collision`: DECIMAL(20, 0),
    `behavior_ran_red_light`: DECIMAL(20, 0),
    `behavior_speeding`: DECIMAL(20, 0),
    `behavior_following_distance_severe`: DECIMAL(20, 0),
    `behavior_following_distance_moderate`: DECIMAL(20, 0),
    `behavior_defensive_driving`: DECIMAL(20, 0),
    `behavior_obstructed_camera`: DECIMAL(20, 0),
    `behavior_lane_departure`: DECIMAL(20, 0),
    `behavior_eating_drinking`: DECIMAL(20, 0),
    `behavior_smoking`: DECIMAL(20, 0),
    `behavior_no_mask`: DECIMAL(20, 0),
    `speeding_light_speeding`: DECIMAL(20, 0),
    `speeding_moderate_speeding`: DECIMAL(20, 0),
    `speeding_heavy_speeding`: DECIMAL(20, 0),
    `speeding_severe_speeding`: DECIMAL(20, 0),
    `adas_following_distance`: DECIMAL(20, 0),
    `adas_distracted_driving`: DECIMAL(20, 0),
    `behavior_forward_collision_warning`: DECIMAL(20, 0),
    `speeding_max_speed`: DECIMAL(20, 0),
    `context_vulnerable_road_user`: DECIMAL(20, 0),
    `context_snowy_or_icy`: DECIMAL(20, 0),
    `context_wet`: DECIMAL(20, 0),
    `context_foggy`: DECIMAL(20, 0),
    `context_construction_or_work_zone`: DECIMAL(20, 0),
    `behavior_railroad_crossing`: DECIMAL(20, 0)
  >,
  `selected_behavior_presets`: STRUCT<
    `harsh_acceleration`: INT,
    `harsh_braking`: INT,
    `harsh_turn`: INT,
    `crash`: INT,
    `harsh_rolling_stop`: INT,
    `vulnerable_road_user`: INT,
    `behavior_generic_distraction`: INT,
    `behavior_drowsy`: INT,
    `behavior_late_response`: INT,
    `behavior_mobile_usage`: INT,
    `behavior_no_seat_belt`: INT,
    `behavior_did_not_yield`: INT,
    `behavior_near_collision`: INT,
    `behavior_ran_red_light`: INT,
    `behavior_speeding`: INT,
    `behavior_following_distance_severe`: INT,
    `behavior_following_distance_moderate`: INT,
    `behavior_defensive_driving`: INT,
    `behavior_obstructed_camera`: INT,
    `behavior_lane_departure`: INT,
    `behavior_eating_drinking`: INT,
    `behavior_smoking`: INT,
    `behavior_no_mask`: INT,
    `speeding_light_speeding`: INT,
    `speeding_moderate_speeding`: INT,
    `speeding_heavy_speeding`: INT,
    `speeding_severe_speeding`: INT,
    `adas_following_distance`: INT,
    `adas_distracted_driving`: INT,
    `behavior_forward_collision_warning`: INT,
    `speeding_max_speed`: INT,
    `context_vulnerable_road_user`: INT,
    `context_snowy_or_icy`: INT,
    `context_wet`: INT,
    `context_foggy`: INT,
    `context_construction_or_work_zone`: INT,
    `behavior_railroad_crossing`: INT
  >,
  `calibrated_behavior_presets`: STRUCT<
    `harsh_acceleration`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `harsh_braking`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `harsh_turn`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `crash`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `harsh_rolling_stop`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `vulnerable_road_user`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_generic_distraction`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_drowsy`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_late_response`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_mobile_usage`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_no_seat_belt`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_did_not_yield`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_near_collision`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_ran_red_light`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_speeding`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_following_distance_severe`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_following_distance_moderate`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_defensive_driving`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_obstructed_camera`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_lane_departure`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_eating_drinking`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_smoking`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_no_mask`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `speeding_light_speeding`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `speeding_moderate_speeding`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `speeding_heavy_speeding`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `speeding_severe_speeding`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `adas_following_distance`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `adas_distracted_driving`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_forward_collision_warning`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `speeding_max_speed`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `context_vulnerable_road_user`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `context_snowy_or_icy`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `context_wet`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `context_foggy`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `context_construction_or_work_zone`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >,
    `behavior_railroad_crossing`: STRUCT<
      `low`: DECIMAL(20, 0),
      `moderate`: DECIMAL(20, 0),
      `high`: DECIMAL(20, 0)
    >
  >,
  `onboarding_completed`: BOOLEAN,
  `selected_fleet_type`: STRUCT<`value`: INT>,
  `custom_performance_bands`: STRUCT<
    `enabled`: BOOLEAN,
    `low_value`: FLOAT,
    `high_value`: FLOAT
  >,
  `enable_use_speeding_durations_in_reporting`: BOOLEAN,
  `context_vulnerable_road_user_weight`: DOUBLE,
  `context_snowy_or_icy_weight`: DOUBLE,
  `context_wet_weight`: DOUBLE,
  `context_foggy_weight`: DOUBLE,
  `context_construction_or_work_zone_weight`: DOUBLE,
  `enable_point_in_time_weights`: BOOLEAN,
  `point_in_time_start_date_ms`: BIGINT
>,
`_raw_detail_proto` STRING,
`date` STRING
