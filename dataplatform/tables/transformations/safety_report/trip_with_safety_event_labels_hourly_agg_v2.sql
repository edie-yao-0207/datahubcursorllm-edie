WITH safety_event_labels_with_interval AS (
  SELECT
    *,
    WINDOW (
    FROM_UNIXTIME(trip_end_ms / CAST(1e3 AS DECIMAL(4,0))),
    '1 hour'
).start AS interval_start
FROM
  safety_report.trip_with_safety_event_labels_v2
WHERE
  -- safety_report.trip_with_safety_event_labels_v2.date is base on trip_end_ms
  date >= ${start_date}
  AND date < ${end_date}
),

safety_event_labels_device_interval AS (
  SELECT
    org_id,
    device_id AS object_id,
    1 AS object_type,       -- vehicle aggregation
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
  FROM
    safety_event_labels_with_interval
),

safety_event_labels_driver_interval AS (
  SELECT
    org_id,
    -- driver id could be null
    COALESCE(driver_id, 0) AS object_id,
    5 AS object_type,       -- driver aggregation
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
)

SELECT
  *
FROM
  (
    SELECT
      *
    FROM
      object_safety_event_labels_interval_agg
  ) PIVOT (
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
      33 severe_speeding_count
    )
  )
