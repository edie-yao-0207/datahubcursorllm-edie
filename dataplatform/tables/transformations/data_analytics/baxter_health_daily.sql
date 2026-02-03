WITH joined_baxter_health_daily_metadata AS ( 
  SELECT *
  FROM data_analytics.baxter_health_daily_metadata 
  UNION ALL
  SELECT *
  FROM data_analytics_from_eu_west_1.baxter_health_daily_metadata
)

SELECT
  date,
  org_id,
  product_id, 
  vg_device_id,
  last_reported_vg_build,
  cm_product_id,
  cm_device_id,
  last_reported_cm_build,
  date_baxter_first_connected,
  attribute_created_at_ts,
  attribute_type,
  daily_trip_duration_ms,
  cm3x_daily_connected_duration_ms,
  cm3x_daily_recording_duration_ms,
  cm3x_daily_connected_uptime_percentage,
  cm3x_daily_recording_uptime_percentage,
  avg_cm3x_trip_connected_uptime_percentage,
  avg_cm3x_trip_recording_uptime_percentage,
  baxter_daily_connected_duration_ms, 
  baxter_daily_recording_duration_ms,
  baxter_daily_grace_recording_duration_ms,
  baxter_daily_connected_uptime_percentage,
  baxter_daily_recording_uptime_percentage,
  baxter_daily_grace_recording_uptime_percentage,
  avg_baxter_trip_connected_uptime_percentage,
  avg_baxter_trip_recording_uptime_percentage, 
  avg_baxter_trip_grace_recording_uptime_percentage,
  daily_forward_framerate,
  daily_inward_framerate,
  daily_rear_framerate,
  total_cm_bootcount,
  total_cm_anomalies,
  cm_daily_max_cpu_gold_temp_celsius,
  cm_daily_avg_cpu_gold_temp_celsius,
  cm_total_overheated_count,
  cm_total_safe_mode_count,
  cm_total_connected_duration_ms,
  cm_total_grace_recording_duration_ms,
  avg_vg_total_cpu_usage,
  avg_cm_total_cpu_usage,
  usb_restart_count
FROM joined_baxter_health_daily_metadata
WHERE date >= ${start_date}
  AND date < ${end_date}
