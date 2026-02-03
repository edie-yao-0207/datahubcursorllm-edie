with speed_way_data as (
  select
    org_id,
    date,
    value.way_id as way_id,
    DATE_TRUNC('hour', FROM_UNIXTIME(value.time / 1000)) AS interval_start,
    coalesce(
      value.ecu_speed_meters_per_second,
      value.gps_speed_meters_per_second
    ) * 2.23694 as speed_mph,
    coalesce(
      value.ecu_speed_meters_per_second,
      value.gps_speed_meters_per_second
    ) * 3.6 as speed_kmph,
    --- Rounding here is to make speed limits multiple of 5. This is because speed
    --- limits are usually a multiple of 5 in the real world.
    round(value.speed_limit_meters_per_second * 2.23694 / 5, 0) * 5 as rounded_speed_limit_mph,
    round(value.speed_limit_meters_per_second * 3.6 / 5, 0) * 5 as rounded_speed_limit_kmph
  from
    kinesisstats.location
  where
    -- limiting the orgs we collect data for the MVP of the report
    org_id in (
      36372, 28464, 19580, 70569, 562949953422075, 16321, -- PSE, Gravity, GoodBros, PODS, MGroup, DH Pace
      31577, 8697, 25038, 9285  -- Clean Harbors, Bragg, Two men and A Truck, Cardiff
    ) and
    value.has_speed_limit is true and
    date < ${end_date} and
    date >= ${start_date}
),

speed_way_data_with_speeding_categories as (
  select *,
  CASE
    WHEN (speed_mph - rounded_speed_limit_mph) > 0
    AND (speed_mph - rounded_speed_limit_mph) < 5 THEN 1
    ELSE 0
  END AS is_light_speeding_mph,
  CASE
    WHEN (speed_kmph - rounded_speed_limit_kmph) > 0
    AND (speed_kmph - rounded_speed_limit_kmph) < 5 THEN 1
    ELSE 0
  END AS is_light_speeding_kmph,
  CASE
    WHEN (speed_mph - rounded_speed_limit_mph) >= 5
    AND (speed_mph - rounded_speed_limit_mph) < 10 THEN 1
    ELSE 0
  END AS is_moderate_speeding_mph,
  CASE
    WHEN (speed_kmph - rounded_speed_limit_kmph) >= 5
    AND (speed_kmph - rounded_speed_limit_kmph) < 10 THEN 1
    ELSE 0
  END AS is_moderate_speeding_kmph,
  CASE
    WHEN (speed_mph - rounded_speed_limit_mph) >= 10
    AND (speed_mph - rounded_speed_limit_mph) < 15 THEN 1
    ELSE 0
  END AS is_heavy_speeding_mph,
  CASE
    WHEN (speed_kmph - rounded_speed_limit_kmph) >= 10
    AND (speed_kmph - rounded_speed_limit_kmph) < 15 THEN 1
    ELSE 0
  END AS is_heavy_speeding_kmph,
  CASE
    WHEN (speed_mph - rounded_speed_limit_mph) >= 15 THEN 1
    ELSE 0
  END AS is_severe_speeding_mph,
  CASE
    WHEN (speed_kmph - rounded_speed_limit_kmph) >= 15 THEN 1
    ELSE 0
  END AS is_severe_speeding_kmph
  FROM speed_way_data
),

org_speeding_aggregations as (
  select
    org_id,
    interval_start,
    sum(is_light_speeding_mph) as total_org_light_speeding_mph,
    sum(is_moderate_speeding_mph) as total_org_moderate_speeding_mph,
    sum(is_heavy_speeding_mph) as total_org_heavy_speeding_mph,
    sum(is_severe_speeding_mph) as total_org_severe_speeding_mph,

    sum(is_light_speeding_kmph) as total_org_light_speeding_kmph,
    sum(is_moderate_speeding_kmph) as total_org_moderate_speeding_kmph,
    sum(is_heavy_speeding_kmph) as total_org_heavy_speeding_kmph,
    sum(is_severe_speeding_kmph) as total_org_severe_speeding_kmph
  from
    speed_way_data_with_speeding_categories
  group by org_id, interval_start
),

reduced_speed_way_with_speeding_categories as (
  select *
  from
    speed_way_data_with_speeding_categories
  where
    is_light_speeding_mph > 0 or
    is_light_speeding_kmph > 0 or
    is_moderate_speeding_mph > 0 or
    is_moderate_speeding_kmph > 0 or
    is_heavy_speeding_mph > 0 or
    is_heavy_speeding_kmph > 0 or
    is_severe_speeding_mph > 0 or
    is_severe_speeding_kmph > 0
),

speed_incident_aggregations as (
  select
    org_id,
    way_id,
    date,
    interval_start,
    sum(speed_mph) as summed_observed_speed_mph,
    count(*) as total_speeding_incidents,

    sum(is_light_speeding_mph) as light_speeding_count_mph,
    sum(is_light_speeding_kmph) as light_speeding_count_kmph,
    sum(is_moderate_speeding_mph) as moderate_speeding_count_mph,
    sum(is_moderate_speeding_kmph) as moderate_speeding_count_kmph,
    sum(is_heavy_speeding_mph) as heavy_speeding_count_mph,
    sum(is_heavy_speeding_kmph) as heavy_speeding_count_kmph,
    sum(is_severe_speeding_mph) as severe_speeding_count_mph,
    sum(is_severe_speeding_kmph) as severe_speeding_count_kmph
  from
    reduced_speed_way_with_speeding_categories
  group by
    org_id,
    way_id,
    date,
    interval_start
),

speed_incident_aggregations_joined as (
  select
    *
  from
    speed_incident_aggregations as way_agg
    join org_speeding_aggregations as o_agg
    using(org_id, interval_start)
)

select * from speed_incident_aggregations_joined
