-- limit join to given date range
-- limit trip version to 101 (current trip version)
-- limit release_stage to GA
WITH date_partitioned_manual_safety_event_labels_hourly_agg AS (
  SELECT
    org_id,
    object_id,
    object_type,
    interval_start,
    date,

    SUM(COALESCE(acceleration_count, 0)) as acceleration_count,
    SUM(COALESCE(braking_count, 0)) as braking_count,
    SUM(COALESCE(harsh_turn_count, 0)) as harsh_turn_count,
    SUM(COALESCE(crash_count, 0)) as crash_count,
    SUM(COALESCE(rolling_stop_count, 0)) as rolling_stop_count,
    SUM(COALESCE(near_collison_count, 0)) as near_collision_count,
    SUM(COALESCE(rollover_protection_count, 0)) as rollover_protection_count,
    SUM(COALESCE(yaw_control_count, 0)) as yaw_control_count,
    SUM(COALESCE(drowsy_count, 0)) as drowsy_count,
    SUM(COALESCE(late_response_count, 0)) as late_response_count,
    SUM(COALESCE(did_not_yield_count, 0)) as did_not_yield_count,
    SUM(COALESCE(no_seatbelt_count, 0)) as no_seatbelt_count,
    SUM(COALESCE(ran_red_light_count, 0)) as ran_red_light_count,
    SUM(COALESCE(speeding_count, 0)) as speeding_count,
    SUM(COALESCE(following_distance_severe_count, 0)) as following_distance_severe_count,
    SUM(COALESCE(following_distance_moderate_count, 0)) as following_distance_moderate_count,
    SUM(COALESCE(generic_tailgating_count, 0)) as generic_tailgating_count,
    SUM(COALESCE(forward_collision_warning_count, 0)) as forward_collision_warning_count, -- edge_forward_collision_warning_count in score_events_report_v2
    SUM(COALESCE(obstructed_camera_count, 0)) as obstructed_camera_count,
    SUM(COALESCE(lane_departure_count, 0)) as lane_departure_count,
    SUM(COALESCE(eating_drinking_count, 0)) as eating_drinking_count,
    SUM(COALESCE(smoking_count, 0)) as smoking_count,
    SUM(COALESCE(max_speed_count, 0)) as max_speed_count,
    SUM(COALESCE(vulnerable_road_user_count, 0)) as vulnerable_road_user_count,
    SUM(COALESCE(edge_railroad_crossing_violation_count, 0)) as edge_railroad_crossing_violation_count,
    /* Tailgating is deprecated, so in score_events_report_v2, we surfaced the generic tailgating count with following distance count.*/
    SUM(COALESCE(following_distance_count, 0)) + SUM(COALESCE(generic_tailgating_count, 0)) as following_distance_count, -- edge_tailgating_count in score_events_report_v2
    SUM(COALESCE(defensive_driving_count, 0)) as defensive_driving_count,
    SUM(COALESCE(policy_violation_mask_count, 0)) as policy_violation_mask_count, -- generic_no_mask_count in score_events_report_v2

    -- Manual-only events
    SUM(COALESCE(generic_distraction_count, 0)) as generic_distraction_count,
    SUM(COALESCE(mobile_usage_count, 0)) as mobile_usage_count,

    -- Duration based events
    SUM(COALESCE(max_speed_duration, 0)) as max_speed_duration

  FROM safety_report.safety_event_labels_hourly_agg_v2
  WHERE date >= ${start_date}
    AND date < ${end_date}
    AND trip_version = 101
    AND release_stage = 100
    /*
      Filter out automatic events where label source is in one of SYSTEM (1) or DEVICE (5)
    */
    AND label_source NOT IN (1, 5)
    AND in_recognition_states = 0
    AND in_dismissed_states = 0
  GROUP BY
    org_id,
    object_id,
    object_type,
    interval_start,
    date
),

-- recognized events are otherwise excluded, we want to only count the defensive driving counts for these
date_partitioned_safety_event_labels_hourly_agg_recognized AS (
  SELECT
    s.org_id,
    s.object_id,
    s.object_type,
    s.interval_start,
    s.date,
    SUM(COALESCE(s.defensive_driving_count, 0)) as defensive_driving_count
  FROM safety_report.safety_event_labels_hourly_agg_v2 s
  INNER JOIN clouddb.organizations o
  ON o.id = s.org_id AND o.release_type_enum IN (0,1,2)
  WHERE s.date >= ${start_date}
    AND s.date < ${end_date}
    AND s.trip_version = 101
    AND s.release_stage = 100
    AND s.label_source IS NOT NULL
    AND s.in_recognition_states = 1
    AND s.in_dismissed_states = 0
    AND s.defensive_driving_count > 0
  GROUP BY
    s.org_id,
    s.object_id,
    s.object_type,
    s.interval_start,
    s.date
),

-- date_partitioned_manual_safety_event_labels_hourly_agg where label source is automatic
date_partitioned_automatic_safety_event_labels_hourly_agg AS (
  SELECT
    org_id,
    object_id,
    object_type,
    interval_start,
    date,

    SUM(COALESCE(acceleration_count, 0)) as acceleration_count,
    SUM(COALESCE(braking_count, 0)) as braking_count,
    SUM(COALESCE(harsh_turn_count, 0)) as harsh_turn_count,
    SUM(COALESCE(crash_count, 0)) as crash_count,
    SUM(COALESCE(rolling_stop_count, 0)) as rolling_stop_count,
    SUM(COALESCE(near_collison_count, 0)) as near_collision_count,
    SUM(COALESCE(rollover_protection_count, 0)) as rollover_protection_count,
    SUM(COALESCE(yaw_control_count, 0)) as yaw_control_count,
    SUM(COALESCE(drowsy_count, 0)) as drowsy_count,
    SUM(COALESCE(late_response_count, 0)) as late_response_count,
    SUM(COALESCE(did_not_yield_count, 0)) as did_not_yield_count,
    SUM(COALESCE(no_seatbelt_count, 0)) as no_seatbelt_count,
    SUM(COALESCE(ran_red_light_count, 0)) as ran_red_light_count,
    SUM(COALESCE(speeding_count, 0)) as speeding_count,
    SUM(COALESCE(following_distance_severe_count, 0)) as following_distance_severe_count,
    SUM(COALESCE(following_distance_moderate_count, 0)) as following_distance_moderate_count,
    SUM(COALESCE(generic_tailgating_count, 0)) as generic_tailgating_count,
    SUM(COALESCE(obstructed_camera_count, 0)) as obstructed_camera_count,
    SUM(COALESCE(lane_departure_count, 0)) as lane_departure_count,
    SUM(COALESCE(eating_drinking_count, 0)) as eating_drinking_count,
    SUM(COALESCE(smoking_count, 0)) as smoking_count,
    SUM(COALESCE(defensive_driving_count, 0)) as defensive_driving_count,
    SUM(COALESCE(policy_violation_mask_count, 0)) as policy_violation_mask_count, -- generic_no_mask_count in score_events_report_v2
    SUM(COALESCE(max_speed_count, 0)) as max_speed_count,
    SUM(COALESCE(vulnerable_road_user_count, 0)) as vulnerable_road_user_count,
    SUM(COALESCE(edge_railroad_crossing_violation_count, 0)) as edge_railroad_crossing_violation_count,
    -- Automatic-only events
    SUM(COALESCE(generic_distraction_count, 0)) as edge_distracted_driving_count,
    SUM(COALESCE(mobile_usage_count, 0)) as edge_mobile_usage_count,
    SUM(COALESCE(forward_collision_warning_count, 0)) as forward_collision_warning_count, -- edge_forward_collision_warning_count in score_events_report_v2
    SUM(COALESCE(following_distance_count, 0)) as following_distance_count, -- edge_tailgating_count in score_events_report_v2

    -- Duration based events
    SUM(COALESCE(max_speed_duration, 0)) as max_speed_duration

  FROM safety_report.safety_event_labels_hourly_agg_v2
  WHERE date >= ${start_date}
    AND date < ${end_date}
    AND trip_version = 101
    AND release_stage = 100
    /*
      Keep events where label source is in one of SYSTEM (1) or DEVICE (5)
    */
    AND label_source IN (1, 5)
    AND in_recognition_states = 0
    AND in_dismissed_states = 0
  GROUP BY
    org_id,
    object_id,
    object_type,
    interval_start,
    date
),

date_partitioned_trip_level_event_counts_hourly_agg AS (
 SELECT
   org_id,
   object_id,
   object_type,
   interval_start,
   date,

   SUM(COALESCE(obstructed_camera_count, 0)) as obstructed_camera_trip_count,
   SUM(COALESCE(no_seatbelt_count, 0)) as no_seatbelt_trip_count,
   SUM(COALESCE(policy_violation_mask_count, 0)) as no_mask_trip_count

 FROM safety_report.trip_with_safety_event_labels_hourly_agg_v2
 WHERE date >= ${start_date}
   AND date < ${end_date}
   AND trip_version = 101
   AND release_stage = 100
   -- we only want automatic labels
   AND label_source in (1)
   AND in_dismissed_states = 0
 GROUP BY
   org_id,
   object_id,
   object_type,
   interval_start,
   date
),

date_partitioned_trip_stats_hourly_agg AS (
 SELECT *
 FROM safety_report.trip_stats_hourly_agg
 WHERE safety_report.trip_stats_hourly_agg.date >= ${start_date}
   AND safety_report.trip_stats_hourly_agg.date < ${end_date}
   AND safety_report.trip_stats_hourly_agg.version = 101
),

-- We are using this table to get total_event_count
date_partitioned_total_safety_event_hourly_agg AS (
  SELECT
    org_id,
    object_id,
    object_type,
    interval_start,
    date,

    SUM(COALESCE(total_event_count, 0)) as total_event_count

  FROM safety_report.safety_event_count_hourly_agg
  WHERE date >= ${start_date}
    AND date < ${end_date}
    AND trip_version = 101
    AND release_stage = 100
    AND in_recognition_states = 0
    AND in_dismissed_states = 0
  GROUP BY
    org_id,
    object_id,
    object_type,
    interval_start,
    date
),

-- We are using this table to get harsh_event_trigger_count
date_partitioned_harsh_safety_event_hourly_agg AS (
  SELECT
    org_id,
    object_id,
    object_type,
    interval_start,
    date,

    SUM(
      acceleration_count + braking_count + harsh_turn_count + crash_count
    ) as harsh_event_trigger_count

  FROM date_partitioned_automatic_safety_event_labels_hourly_agg
  GROUP BY
    org_id,
    object_id,
    object_type,
    interval_start,
    date
)

SELECT
  -- score_events_report_v3 primary key
  org_id,
  object_id,
  object_type,
  interval_start,
  date,

  -- TODO: add trip stats
  COALESCE(date_partitioned_harsh_safety_event_hourly_agg.harsh_event_trigger_count, 0) AS harsh_event_trigger_count,
  COALESCE(date_partitioned_total_safety_event_hourly_agg.total_event_count, 0) AS total_event_count,
  COALESCE(trip_count, 0) AS trip_count,
  COALESCE(engine_idle_ms, 0) AS engine_idle_ms,
  COALESCE(distance_meters, 0) AS distance_meters,
  COALESCE(driving_time_ms, 0) AS driving_time_ms,
  COALESCE(seatbelt_reporting_present_ms, 0) AS seatbelt_reporting_present_ms,
  COALESCE(seatbelt_unbuckled_ms, 0) AS seatbelt_unbuckled_ms,

  COALESCE(date_partitioned_trip_stats_hourly_agg.not_speeding_ms, 0) as not_speeding_ms,
  COALESCE(date_partitioned_trip_stats_hourly_agg.light_speeding_ms, 0) as light_speeding_ms,
  COALESCE(date_partitioned_trip_stats_hourly_agg.moderate_speeding_ms, 0) as moderate_speeding_ms,
  COALESCE(date_partitioned_trip_stats_hourly_agg.heavy_speeding_ms, 0) as heavy_speeding_ms,
  COALESCE(date_partitioned_trip_stats_hourly_agg.severe_speeding_ms, 0) as severe_speeding_ms,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.max_speed_duration, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.max_speed_duration, 0) as max_speed_duration,

  COALESCE(date_partitioned_trip_stats_hourly_agg.not_speeding_count, 0) as not_speeding_count,
  COALESCE(date_partitioned_trip_stats_hourly_agg.light_speeding_count, 0) as light_speeding_count,
  COALESCE(date_partitioned_trip_stats_hourly_agg.moderate_speeding_count, 0) as moderate_speeding_count,
  COALESCE(date_partitioned_trip_stats_hourly_agg.heavy_speeding_count, 0) as heavy_speeding_count,
  COALESCE(date_partitioned_trip_stats_hourly_agg.severe_speeding_count, 0) as severe_speeding_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.max_speed_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.max_speed_count, 0) as max_speed_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.vulnerable_road_user_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.vulnerable_road_user_count, 0) as vulnerable_road_user_count,

  --Camera-equipped vehicle metrics
  COALESCE(date_partitioned_trip_stats_hourly_agg.outward_camera_distance_meters, 0) as outward_camera_distance_meters,
  COALESCE(date_partitioned_trip_stats_hourly_agg.inward_outward_camera_distance_meters, 0) as inward_outward_camera_distance_meters,
  COALESCE(date_partitioned_trip_stats_hourly_agg.outward_camera_driving_time_ms, 0) as outward_camera_driving_time_ms,
  COALESCE(date_partitioned_trip_stats_hourly_agg.inward_outward_camera_driving_time_ms, 0) as inward_outward_camera_driving_time_ms,

  -- TODO: add safety event label count stats
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.acceleration_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.acceleration_count, 0) as acceleration_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.braking_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.braking_count, 0) as braking_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.harsh_turn_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.harsh_turn_count, 0) as harsh_turn_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.crash_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.crash_count, 0) as crash_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.rolling_stop_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.rolling_stop_count, 0) as rolling_stop_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.near_collision_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.near_collision_count, 0) as near_collision_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.rollover_protection_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.rollover_protection_count, 0) as rollover_protection_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.yaw_control_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.yaw_control_count, 0) as yaw_control_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.drowsy_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.drowsy_count, 0) as drowsy_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.late_response_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.late_response_count, 0) as late_response_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.did_not_yield_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.did_not_yield_count, 0) as did_not_yield_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.no_seatbelt_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.no_seatbelt_count, 0) as no_seatbelt_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.ran_red_light_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.ran_red_light_count, 0) as ran_red_light_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.speeding_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.speeding_count, 0) as speeding_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.following_distance_severe_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.following_distance_severe_count, 0) as following_distance_severe_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.following_distance_moderate_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.following_distance_moderate_count, 0) as following_distance_moderate_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.generic_tailgating_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.generic_tailgating_count, 0) as generic_tailgating_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.obstructed_camera_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.obstructed_camera_count, 0) as obstructed_camera_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.lane_departure_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.lane_departure_count, 0) as lane_departure_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.eating_drinking_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.eating_drinking_count, 0)as eating_drinking_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.smoking_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.smoking_count, 0) as smoking_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.policy_violation_mask_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.policy_violation_mask_count, 0) as policy_violation_mask_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.defensive_driving_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.defensive_driving_count, 0) + COALESCE(date_partitioned_safety_event_labels_hourly_agg_recognized.defensive_driving_count, 0) as defensive_driving_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.following_distance_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.following_distance_count, 0) as following_distance_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.edge_railroad_crossing_violation_count, 0) + COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.edge_railroad_crossing_violation_count, 0) as edge_railroad_crossing_violation_count,
  -- Forward Collision Warning used to be automatic-only. It is now both automatic and manual
  COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.forward_collision_warning_count, 0) + COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.forward_collision_warning_count, 0)  as forward_collision_warning_count,

  -- Automatic-only events
  COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.edge_distracted_driving_count, 0) as edge_distracted_driving_count,
  COALESCE(date_partitioned_automatic_safety_event_labels_hourly_agg.edge_mobile_usage_count, 0) as edge_mobile_usage_count,

  -- Manual-only events
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.mobile_usage_count, 0) as mobile_usage_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.generic_distraction_count, 0) as generic_distraction_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.drowsy_count, 0) as manual_drowsy_count,
  COALESCE(date_partitioned_manual_safety_event_labels_hourly_agg.lane_departure_count, 0) as manual_lane_departure_count,

  -- TODO: add trip safety event label count stats
  COALESCE(date_partitioned_trip_level_event_counts_hourly_agg.obstructed_camera_trip_count, 0) as obstructed_camera_trip_count,
  COALESCE(date_partitioned_trip_level_event_counts_hourly_agg.no_seatbelt_trip_count, 0) as no_seatbelt_trip_count,
  COALESCE(date_partitioned_trip_level_event_counts_hourly_agg.no_mask_trip_count, 0) as no_mask_trip_count

FROM
  date_partitioned_manual_safety_event_labels_hourly_agg
  FULL OUTER JOIN date_partitioned_automatic_safety_event_labels_hourly_agg USING (org_id, object_id, object_type, date, interval_start)
  FULL OUTER JOIN date_partitioned_trip_level_event_counts_hourly_agg USING (org_id, object_id, object_type, date, interval_start)
  FULL OUTER JOIN date_partitioned_trip_stats_hourly_agg USING (org_id, object_id, object_type, date, interval_start)
  FULL OUTER JOIN date_partitioned_total_safety_event_hourly_agg USING (org_id, object_id, object_type, date, interval_start)
  FULL OUTER JOIN date_partitioned_harsh_safety_event_hourly_agg USING (org_id, object_id, object_type, date, interval_start)
  FULL OUTER JOIN date_partitioned_safety_event_labels_hourly_agg_recognized USING (org_id, object_id, object_type, date, interval_start)
