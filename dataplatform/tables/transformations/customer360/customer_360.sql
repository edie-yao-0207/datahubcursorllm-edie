WITH customer_operations AS (
  SELECT
    IFNULL(us.sam_number, eu.sam_number) AS sam_number,
    IFNULL(us.date,eu.date) AS date,
    IFNULL(us.num_returned_vg, eu.num_returned_vg) AS num_returned_vg,
    IFNULL(us.num_returned_cm, eu.num_returned_cm) AS num_returned_cm,
    IFNULL(us.num_returned_ag, eu.num_returned_ag) AS num_returned_ag,
    IFNULL(us.num_returned_ig, eu.num_returned_ig) AS num_returned_ig,
    IFNULL(us.total_devices_connected, eu.total_devices_connected) AS total_devices_connected,
    IFNULL(us.total_devices_shipped, eu.total_devices_shipped) AS total_devices_shipped
  FROM customer360.customer_operations us
  FULL JOIN customer360_from_eu_west_1.customer_operations eu ON 
    us.sam_number = eu.sam_number AND
    us.date = eu.date
),

org_operations AS (
  SELECT
    IFNULL(us.org_id, eu.org_id) AS org_id,
    IFNULL(us.sam_number, eu.sam_number) AS sam_number,
    IFNULL(us.date,eu.date) AS date,
    IFNULL(us.num_trips, 0) + IFNULL(eu.num_trips, 0) AS num_trips,
    IFNULL(us.total_trip_duration, 0) + IFNULL(eu.total_trip_duration, 0) AS total_trip_duration, 
    IFNULL(us.total_trip_distance_meters, 0) + IFNULL(eu.total_trip_distance_meters, 0) AS total_trip_distance_meters,
    IFNULL(us.total_fuel_consumed_ml, 0) + IFNULL(eu.total_fuel_consumed_ml, 0) AS total_fuel_consumed_ml,
    IFNULL(us.total_not_speeding_ms, 0) + IFNULL(eu.total_not_speeding_ms, 0) AS total_not_speeding_ms,
    IFNULL(us.total_light_speeding_ms, 0) + IFNULL(eu.total_light_speeding_ms, 0) AS total_light_speeding_ms,
    IFNULL(us.total_moderate_speeding_ms, 0) + IFNULL(eu.total_moderate_speeding_ms, 0) AS total_moderate_speeding_ms,
    IFNULL(us.total_heavy_speeding_ms, 0) + IFNULL(eu.total_heavy_speeding_ms, 0) AS total_heavy_speeding_ms,
    IFNULL(us.total_severe_speeding_ms, 0) + IFNULL(eu.total_severe_speeding_ms, 0) AS total_severe_speeding_ms,
    IFNULL(us.total_harsh_events, 0) + IFNULL(eu.total_harsh_events, 0) AS total_harsh_events,
    IFNULL(us.total_data_usage, 0) + IFNULL(eu.total_data_usage, 0) AS total_data_usage,
    IFNULL(us.num_active_vg, 0) + IFNULL(eu.num_active_vg, 0) AS num_active_vg,
    IFNULL(us.num_active_cm, 0) + IFNULL(eu.num_active_cm, 0) AS num_active_cm,
    IFNULL(us.num_active_ag, 0) + IFNULL(eu.num_active_ag, 0) AS num_active_ag,
    IFNULL(us.num_active_ig, 0) + IFNULL(eu.num_active_ig, 0) AS num_active_ig,
    IFNULL(us.haBraking, 0) + IFNULL(eu.haBraking, 0) AS haBraking,
    IFNULL(us.haAccel, 0) + IFNULL(eu.haAccel, 0) AS haAccel,
    IFNULL(us.haSharpTurn, 0) + IFNULL(eu.haSharpTurn, 0) AS haSharpTurn,
    IFNULL(us.haCrash, 0) + IFNULL(eu.haCrash, 0) AS haCrash,
    IFNULL(us.haDistractedDriving, 0) + IFNULL(eu.haDistractedDriving, 0) AS haDistractedDriving,
    IFNULL(us.haTailgating, 0) + IFNULL(eu.haTailgating, 0) AS haTailgating,
    IFNULL(us.needs_review, 0) + IFNULL(eu.needs_review, 0) AS needs_review,
    IFNULL(us.needs_coaching, 0) + IFNULL(eu.needs_coaching, 0) AS needs_coaching,
    IFNULL(us.needs_recognition, 0) + IFNULL(eu.needs_recognition, 0) AS needs_recognition,
    IFNULL(us.reviewed, 0) + IFNULL(eu.reviewed, 0) AS reviewed,
    IFNULL(us.coached, 0) + IFNULL(eu.coached, 0) AS coached,
    IFNULL(us.recognized, 0) + IFNULL(eu.recognized, 0) AS recognized,
    IFNULL(us.dismissed, 0) + IFNULL(eu.dismissed, 0) AS dismissed,
    IFNULL(us.num_videos_retrieved, 0) + IFNULL(eu.num_videos_retrieved, 0) AS num_videos_retrieved
  FROM customer360.org_operations us
  FULL JOIN customer360_from_eu_west_1.org_operations eu ON 
    us.org_id = eu.org_id AND
    us.date = eu.date
),

customer_compliance AS (
  SELECT
    IFNULL(us.org_id, eu.org_id) AS org_id,
    IFNULL(us.sam_number, eu.sam_number) AS sam_number,
    IFNULL(us.date,eu.date) AS date,
    IFNULL(us.num_unassigned_segments, 0) + IFNULL(eu.num_unassigned_segments, 0) AS num_unassigned_segments,
    IFNULL(us.unassigned_driver_time_hours, 0) + IFNULL(eu.unassigned_driver_time_hours, 0) AS unassigned_driver_time_hours
  FROM customer360.org_compliance us
  FULL JOIN customer360_from_eu_west_1.org_compliance eu ON
    us.org_id = eu.org_id AND
    us.date = eu.date
),

customer_dates AS (
  SELECT
    IFNULL(us.org_id, eu.org_id) AS org_id,
    IFNULL(us.sam_number, eu.sam_number) AS sam_number,
    CASE 
      WHEN eu.first_heartbeat_date IS NULL THEN us.first_heartbeat_date
      WHEN us.first_heartbeat_date IS NULL THEN eu.first_heartbeat_date
      WHEN us.first_heartbeat_date < eu.first_heartbeat_date THEN us.first_heartbeat_date 
      ELSE eu.first_heartbeat_date 
    END AS first_heartbeat_date,
    IFNULL(us.date,eu.date) AS date
  FROM customer360.org_dates us
  FULL JOIN customer360_from_eu_west_1.org_dates eu ON
    us.org_id = eu.org_id AND
    us.date = eu.date
),

customer_platform_usage AS (
  SELECT
    IFNULL(us.org_id, eu.org_id) AS org_id,
    IFNULL(us.sam_number, eu.sam_number) AS sam_number,
    IFNULL(us.date,eu.date) AS date,
    IFNULL(us.num_route_loads, 0) + IFNULL(eu.num_route_loads, 0) AS num_route_loads,
    IFNULL(us.num_users, 0) + IFNULL(eu.num_users, 0) AS num_users,
    IFNULL(us.num_driver_app_loads, 0) + IFNULL(eu.num_driver_app_loads, 0) AS num_driver_app_loads,
    IFNULL(us.num_admin_app_loads, 0) + IFNULL(eu.num_admin_app_loads, 0) AS num_admin_app_loads
  FROM customer360.org_platform_usage us
  FULL JOIN customer360_from_eu_west_1.org_platform_usage eu ON
    us.org_id = eu.org_id AND
    us.date = eu.date
),

customer_sophistication AS (
  SELECT
    IFNULL(us.org_id, eu.org_id) AS org_id,
    IFNULL(us.sam_number, eu.sam_number) AS sam_number,
    IFNULL(us.date,eu.date) AS date,
    IFNULL(us.num_scheduled_reports, 0) + IFNULL(eu.num_scheduled_reports, 0) AS num_scheduled_reports,
    IFNULL(us.num_routes, 0) + IFNULL(eu.num_routes, 0) AS num_routes,
    IFNULL(us.num_addresses, 0) + IFNULL(eu.num_addresses, 0) AS num_addresses,
    IFNULL(us.num_alerts, 0) + IFNULL(eu.num_alerts, 0) AS num_alerts,
    IFNULL(us.num_roles, 0) + IFNULL(eu.num_roles, 0) AS num_roles,
    IFNULL(us.num_tags, 0) + IFNULL(eu.num_tags, 0) AS num_tags,
    IFNULL(us.num_cloud_users, 0) + IFNULL(eu.num_cloud_users, 0) AS num_cloud_users,
    IFNULL(us.num_api_calls, 0) + IFNULL(eu.num_api_calls, 0) AS num_api_calls
  FROM customer360.org_sophistication us
  FULL JOIN customer360_from_eu_west_1.org_sophistication eu ON
    us.org_id = eu.org_id AND
    us.date = eu.date
),

customer_health_tracker AS (  
  SELECT
    IFNULL(us.date, eu.date) AS date, 
    IFNULL(us.org_id, eu.org_id) AS org_id,
    IFNULL(us.csm_trend, eu.csm_trend) AS csm_trend,
    IFNULL(us.csm_rating, eu.csm_rating) AS csm_rating,
    IFNULL(us.csm_sentiment_cause, eu.csm_sentiment_cause) AS csm_sentiment_cause,
    IFNULL(us.csm_notes, eu.csm_notes) AS csm_notes
  FROM customer360.customer_health_tracker us
  FULL JOIN customer360_from_eu_west_1.customer_health_tracker eu ON
    us.org_id = eu.org_id AND
    us.date = eu.date
)

SELECT
  cd.*,
  cmt.csm_trend,
  cmt.csm_rating,
  cmt.csm_sentiment_cause,
  cmt.csm_notes,
  oo.num_trips,
  oo.total_trip_duration,
  oo.total_trip_distance_meters,
  oo.total_fuel_consumed_ml,
  oo.total_not_speeding_ms,
  oo.total_light_speeding_ms,
  oo.total_moderate_speeding_ms,
  oo.total_heavy_speeding_ms,
  oo.total_severe_speeding_ms,
  oo.total_harsh_events,
  oo.total_data_usage,
  oo.num_active_vg,
  oo.num_active_cm,
  oo.num_active_ag,
  oo.num_active_ig,
  co.num_returned_vg,
  co.num_returned_cm,
  co.num_returned_ag,
  co.num_returned_ig,
  oo.haBraking,
  oo.haAccel,
  oo.haSharpTurn,
  oo.haCrash,
  oo.haDistractedDriving,
  oo.haTailgating,
  oo.needs_review,
  oo.needs_coaching,
  oo.needs_recognition,
  oo.reviewed,
  oo.coached,
  oo.recognized,
  oo.dismissed,
  oo.num_videos_retrieved,
  cs.assets,
  cs.backend_infrastructure,
  cs.connected_admin,
  cs.connected_driver,
  cs.dashcam,
  cs.eurofleet,
  cs.fleet_app_ecosystem,
  cs.`fleet_management_-_other`,
  cs.hardware,
  cs.industrial,
  cs.multicam,
  cs.`non-product_related`,
  cs.platform,
  cs.safety,
  cs.telematics,
  cs.unknown,
  cpu.num_route_loads,
  cpu.num_users,
  cpu.num_driver_app_loads,
  cpu.num_admin_app_loads,
  cn.avg_raw_score,
  cn.avg_nps_score,
  cso.num_scheduled_reports,
  cso.num_routes,
  cso.num_addresses,
  cso.num_alerts,
  cso.num_roles,
  cso.num_tags,
  cso.num_cloud_users,
  cso.num_api_calls,
  cc.num_unassigned_segments,
  cc.unassigned_driver_time_hours,
  co.total_devices_connected,
  co.total_devices_shipped
FROM customer_dates cd
LEFT JOIN customer_health_tracker cmt ON 
    cmt.org_id = cd.org_id AND 
    cmt.date = cd.date
LEFT JOIN customer_operations co ON
  cd.date = co.date AND 
  cd.sam_number = co.sam_number
LEFT JOIN org_operations oo ON
  cd.date = oo.date AND 
  cd.org_id = oo.org_id
LEFT JOIN customer360.customer_support cs ON
  cd.date = cs.date AND 
  cd.sam_number = cs.sam_number
LEFT JOIN customer_platform_usage cpu ON
  cd.date = cpu.date AND 
  cd.org_id = cpu.org_id
LEFT JOIN customer360.customer_nps cn ON
  cd.date = cn.date AND 
  cd.sam_number = cn.sam_number
LEFT JOIN customer_sophistication cso ON
  cd.date = cso.date AND 
  cd.org_id = cso.org_id
LEFT JOIN customer_compliance cc ON
  cd.date = cc.date AND 
  cd.org_id = cc.org_id
WHERE
    cd.date >= ${start_date} AND
    cd.date < ${end_date} AND
    cd.date IS NOT NULL AND
    cd.sam_number IS NOT NULL AND
    cd.org_id IS NOT NULL
