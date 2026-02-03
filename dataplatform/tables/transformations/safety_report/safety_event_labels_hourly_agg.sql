WITH safety_event_labels_with_interval AS (
  SELECT
    *,
    WINDOW (
    FROM_UNIXTIME(trip_end_ms / CAST(1e3 AS DECIMAL(4,0))),
    '1 hour'
).start AS interval_start
FROM
  safety_report.safety_event_labels
WHERE
  -- safety_report.safety_event_labels.date is based on trip_end_ms
    date >= ${start_date}
  AND date < ${end_date}
  ),

safety_event_labels_device_interval AS (
  SELECT
    org_id,
    device_id AS object_id,
    1 AS object_type,
    event_duration,
    interval_start,
    label_type,
    label_source,
    trip_version,
    release_stage,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    model_version,
    date
  FROM
    safety_event_labels_with_interval
),

safety_event_labels_driver_interval AS (
  SELECT
    org_id,
    -- driver id could be null
    COALESCE(driver_id, 0) AS object_id,
    5 AS object_type,
    event_duration,
    interval_start,
    label_type,
    label_source,
    trip_version,
    release_stage,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    model_version,
    date
  FROM
    safety_event_labels_with_interval
),


object_safety_event_labels_interval AS (
  SELECT
    *
  FROM
    safety_event_labels_device_interval
  union all
  SELECT
    *
  FROM
    safety_event_labels_driver_interval
),

-- Aggregate model versions as a set
model_versions_agg AS (
  SELECT
    org_id,
    object_id,
    object_type,
    interval_start,
    label_source,
    trip_version,
    release_stage,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    date,
    ARRAY_SORT(COLLECT_SET(model_version)) AS model_versions
  FROM
    object_safety_event_labels_interval
  GROUP BY
    org_id,
    object_id,
    object_type,
    interval_start,
    label_source,
    trip_version,
    release_stage,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    date
),

-- Number of events of each label type in interval
object_safety_event_labels_interval_agg AS (
  SELECT
    org_id,
    object_id,
    object_type,
    interval_start,
    label_type,
    label_source,
    trip_version,
    release_stage,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    count(*) AS count,
    SUM(event_duration) as total_event_duration,
    date
  FROM
    object_safety_event_labels_interval
  group by
    org_id,
    object_id,
    object_type,
    interval_start,
    label_type,
    label_source,
    trip_version,
    release_stage,
    in_dismissed_states,
    in_recognition_states,
    in_coaching_states,
    in_triage_states,
    date
),

object_safety_event_labels_interval_agg_count_pivot AS (
  SELECT
    *
  FROM
    (
      SELECT
        org_id,
        object_id,
        object_type,
        interval_start,
        label_type,
        label_source,
        trip_version,
        release_stage,
        in_dismissed_states,
        in_recognition_states,
        in_coaching_states,
        in_triage_states,
        count,
        date
      FROM
        object_safety_event_labels_interval_agg
    )
    PIVOT (
      SUM(count) FOR label_type IN (
        0 invalid_count,
        1 generic_tailgating_count,
        2 generic_distraction_count,
        3 defensive_driving_count,
        4 rolling_stop_count,
        5 near_collison_count,
        6 speeding_count,
        7 obstructed_camera_count,
        8 did_not_yield_count,
        9 no_seatbelt_count,
        10 mobile_usage_count,
        11 drowsy_count,
        12 lane_departure_count,
        13 following_distance_severe_count,
        14 following_distance_moderate_count,
        15 late_response_count,
        16 acceleration_count,
        17 braking_count,
        18 harsh_turn_count,
        19 crash_count,
        20 rollover_protection_count,
        21 yaw_control_count,
        22 ran_red_light_count,
        23 forward_collision_warning_count,
        24 eating_drinking_count,
        25 smoking_count,
        26 following_distance_count,
        27 edge_distracted_driving_count,
        28 policy_violation_mask_count,
        29 eating_count,
        30 drinking_count,
        31 edge_railroad_crossing_violation_count,
        32 max_speed_count,
        33 severe_speeding_count,
        47 vulnerable_road_user_count
      )
    )
),

object_safety_event_labels_interval_agg_duration_pivot AS (
  SELECT
    *
  FROM
    (
      SELECT
        org_id,
        object_id,
        object_type,
        interval_start,
        label_type,
        label_source,
        trip_version,
        release_stage,
        in_dismissed_states,
        in_recognition_states,
        in_coaching_states,
        in_triage_states,
        total_event_duration,
        date
      FROM
        object_safety_event_labels_interval_agg
    )
    PIVOT (
      SUM(total_event_duration) FOR label_type IN (
        0 invalid_duration,
        1 generic_tailgating_duration,
        2 generic_distraction_duration,
        3 defensive_driving_duration,
        4 rolling_stop_duration,
        5 near_collison_duration,
        6 speeding_duration,
        7 obstructed_camera_duration,
        8 did_not_yield_duration,
        9 no_seatbelt_duration,
        10 mobile_usage_duration,
        11 drowsy_duration,
        12 lane_departure_duration,
        13 following_distance_severe_duration,
        14 following_distance_moderate_duration,
        15 late_response_duration,
        16 acceleration_duration,
        17 braking_duration,
        18 harsh_turn_duration,
        19 crash_duration,
        20 rollover_protection_duration,
        21 yaw_control_duration,
        22 ran_red_light_duration,
        23 forward_collision_warning_duration,
        24 eating_drinking_duration,
        25 smoking_duration,
        26 following_distance_duration,
        27 edge_distracted_driving_duration,
        28 policy_violation_mask_duration,
        29 eating_duration,
        30 drinking_duration,
        31 edge_railroad_crossing_violation_duration,
        32 max_speed_duration,
        33 severe_speeding_duration,
        47 vulnerable_road_user_duration
      )
    )
)

SELECT
  count.org_id,
  count.object_id,
  count.object_type,
  count.interval_start,
  count.label_source,
  count.trip_version,
  count.release_stage,
  count.in_dismissed_states,
  count.in_recognition_states,
  count.in_coaching_states,
  count.in_triage_states,
  count.date,
  mv.model_versions,
  -- counts
  invalid_count,
  generic_tailgating_count,
  generic_distraction_count,
  defensive_driving_count,
  rolling_stop_count,
  near_collison_count,
  speeding_count,
  obstructed_camera_count,
  did_not_yield_count,
  no_seatbelt_count,
  mobile_usage_count,
  drowsy_count,
  lane_departure_count,
  following_distance_severe_count,
  following_distance_moderate_count,
  late_response_count,
  acceleration_count,
  braking_count,
  harsh_turn_count,
  crash_count,
  rollover_protection_count,
  yaw_control_count,
  ran_red_light_count,
  forward_collision_warning_count,
  eating_drinking_count,
  smoking_count,
  following_distance_count,
  edge_distracted_driving_count,
  policy_violation_mask_count,
  eating_count,
  drinking_count,
  edge_railroad_crossing_violation_count,
  max_speed_count,
  severe_speeding_count,
  vulnerable_road_user_count,
  -- durations
  invalid_duration,
  generic_tailgating_duration,
  generic_distraction_duration,
  defensive_driving_duration,
  rolling_stop_duration,
  near_collison_duration,
  speeding_duration,
  obstructed_camera_duration,
  did_not_yield_duration,
  no_seatbelt_duration,
  mobile_usage_duration,
  drowsy_duration,
  lane_departure_duration,
  following_distance_severe_duration,
  following_distance_moderate_duration,
  late_response_duration,
  acceleration_duration,
  braking_duration,
  harsh_turn_duration,
  crash_duration,
  rollover_protection_duration,
  yaw_control_duration,
  ran_red_light_duration,
  forward_collision_warning_duration,
  eating_drinking_duration,
  smoking_duration,
  following_distance_duration,
  edge_distracted_driving_duration,
  policy_violation_mask_duration,
  eating_duration,
  drinking_duration,
  edge_railroad_crossing_violation_duration,
  max_speed_duration,
  severe_speeding_duration
FROM
  object_safety_event_labels_interval_agg_count_pivot count
LEFT JOIN
  object_safety_event_labels_interval_agg_duration_pivot duration
  ON  count.org_id              = duration.org_id
  AND count.object_id           = duration.object_id
  AND count.object_type         = duration.object_type
  AND count.interval_start      = duration.interval_start
  AND count.label_source        = duration.label_source
  AND count.trip_version        = duration.trip_version
  AND count.release_stage       = duration.release_stage
  AND count.in_dismissed_states = duration.in_dismissed_states
  AND count.in_recognition_states = duration.in_recognition_states
  AND count.in_coaching_states  = duration.in_coaching_states
  AND count.in_triage_states    = duration.in_triage_states
  AND count.date                = duration.date
LEFT JOIN
  model_versions_agg mv
  ON  count.org_id = mv.org_id
  AND count.object_id = mv.object_id
  AND count.object_type = mv.object_type
  AND count.interval_start = mv.interval_start
  AND count.label_source = mv.label_source
  AND count.trip_version = mv.trip_version
  AND count.release_stage = mv.release_stage
  AND count.in_dismissed_states = mv.in_dismissed_states
  AND count.in_recognition_states = mv.in_recognition_states
  AND count.in_coaching_states = mv.in_coaching_states
  AND count.in_triage_states = mv.in_triage_states
  AND count.date = mv.date