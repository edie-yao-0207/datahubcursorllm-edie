WITH osddashcamstreamstatus AS ( 
  SELECT 
    org_id,
    object_id AS cm3x_device_id, -- CMID for CM3X, VGID for CM1X AND CM2X.
    time AS timestamp,
    date,
    FILTER(value.proto_value.dashcam_stream_status.stream_status, stream -> stream.stream_source_enum IS NULL) AS forward_stream_filtered,
    FILTER(value.proto_value.dashcam_stream_status.stream_status, stream -> stream.stream_source_enum == 1) AS inward_stream_filtered,
    FILTER(value.proto_value.dashcam_stream_status.stream_status, stream -> stream.stream_source_enum == 6) AS rear_stream_filtered
  FROM kinesisstats.osddashcamstreamstatus 
  WHERE value.is_databreak = false
    AND value.is_end = false
    AND date >= DATE_SUB(${end_date}, 14)
    AND object_id IN (SELECT cm_device_id FROM data_analytics.baxter_devices)
    AND value.proto_value.dashcam_stream_status.stream_status IS NOT NULL
),

baxter_stream_framerate AS (
  SELECT
    org_id,
    cm3x_device_id,
    timestamp,
    date,
    IF(
      size(forward_stream_filtered) == 1, -- Check if exactly 1 stream was sent up for this direction
      forward_stream_filtered[0].recorded_frames/60, 
      null -- Default to null so the value does not get factored into the daily average
    ) AS forward_framerate,
    IF(
      size(inward_stream_filtered) == 1, 
      inward_stream_filtered[0].recorded_frames/60, 
      null 
    ) AS inward_framerate,
    IF(
      size(rear_stream_filtered) == 1, 
      rear_stream_filtered[0].recorded_frames/60, 
      null 
    ) AS rear_framerate
  FROM osddashcamstreamstatus
  WHERE 
    size(forward_stream_filtered) == 1 OR
    size(inward_stream_filtered) == 1 OR
    size(rear_stream_filtered) == 1 
),

daily_average_framerate AS (
  SELECT 
    date,
    org_id,
    cm3x_device_id,
    ROUND(AVG(forward_framerate), 2) AS daily_forward_framerate,
    ROUND(AVG(inward_framerate), 2) AS daily_inward_framerate,
    ROUND(AVG(rear_framerate), 2) AS daily_rear_framerate
  FROM baxter_stream_framerate
  GROUP BY 
    date,
    org_id,
    cm3x_device_id
),

daily_baxter_trip_uptime AS (
  SELECT 
    date,
    org_id, 
    vg_device_id,
    cm_device_id,
    product_id,
    cm_product_id,
    SUM(baxter_connected_duration_ms) AS baxter_daily_connected_duration_ms,
    SUM(baxter_recording_duration_ms) AS baxter_daily_recording_duration_ms,
    SUM(baxter_grace_recording_duration_ms) AS baxter_daily_grace_recording_duration_ms,
    SUM(cm_connected_duration_ms) AS cm3x_daily_connected_duration_ms,
    SUM(cm_recording_duration_ms) AS cm3x_daily_recording_duration_ms,
    SUM(trip_duration_ms) AS daily_trip_duration_ms,
    AVG(baxter_trip_connected_uptime_percentage) AS avg_baxter_trip_connected_uptime_percentage,
    AVG(baxter_trip_recording_uptime_percentage) AS avg_baxter_trip_recording_uptime_percentage, 
    AVG(baxter_trip_grace_recording_uptime_percentage) AS avg_baxter_trip_grace_recording_uptime_percentage,
    AVG(cm3x_trip_connected_uptime_percentage) AS avg_cm3x_trip_connected_uptime_percentage,
    AVG(cm3x_trip_recording_uptime_percentage) AS avg_cm3x_trip_recording_uptime_percentage
  FROM data_analytics.baxter_health_trip_intervals 
  GROUP BY
    date, -- We can GROUP BY date because dataprep_safety.cm_vg_intervals already breaks trips on the day boundary.
    org_id,
    vg_device_id, 
    cm_device_id,
    product_id,
    cm_product_id
),

daily_baxter_metrics AS (
  SELECT 
    a.*, 
    b.daily_forward_framerate,
    b.daily_inward_framerate,
    b.daily_rear_framerate
  FROM daily_baxter_trip_uptime AS a
  LEFT JOIN daily_average_framerate AS b
    ON a.date = b.date
    AND a.org_id = b.org_id
    AND a.cm_device_id = b.cm3x_device_id
),

cm_device_health_daily AS (
  SELECT * 
  FROM dataprep_safety.cm_device_health_daily
  WHERE date >= DATE_SUB(${end_date}, 14)
    AND cm_device_id IN (SELECT cm_device_id FROM data_analytics.baxter_devices)
),

daily_baxter_health AS (
  SELECT 
    a.*,
    b.total_cm_bootcount,
    b.total_cm_anomalies,
    b.daily_max_cpu_gold_temp_celsius AS cm_daily_max_cpu_gold_temp_celsius,
    b.daily_avg_cpu_gold_temp_celsius AS cm_daily_avg_cpu_gold_temp_celsius,
    b.total_overheated_count AS cm_total_overheated_count,
    b.total_safe_mode_count AS cm_total_safe_mode_count,
    b.last_reported_cm_build, 
    b.last_reported_vg_build,
    b.total_trip_connected_duration_ms AS cm_total_connected_duration_ms,
    b.total_trip_grace_recording_duration_ms AS cm_total_grace_recording_duration_ms,
    ROUND(100 * a.baxter_daily_connected_duration_ms/a.daily_trip_duration_ms, 2) AS baxter_daily_connected_uptime_percentage,
    ROUND(100 * a.baxter_daily_recording_duration_ms/a.daily_trip_duration_ms, 2) AS baxter_daily_recording_uptime_percentage,
    ROUND(100 * a.baxter_daily_grace_recording_duration_ms/a.daily_trip_duration_ms, 2) AS baxter_daily_grace_recording_uptime_percentage,
    ROUND(100 * b.total_trip_connected_duration_ms / b.total_trip_duration_ms, 2) AS cm3x_daily_connected_uptime_percentage, 
    ROUND(100 * b.total_trip_grace_recording_duration_ms / b.total_trip_duration_ms, 2) AS cm3x_daily_recording_uptime_percentage 
  FROM daily_baxter_metrics AS a
  LEFT JOIN cm_device_health_daily AS b
    ON a.org_id = b.org_id
      AND a.vg_device_id = b.device_id
      AND a.cm_device_id = b.cm_device_id
      AND a.date = b.date
),

attributes AS (
  SELECT sub.*
  FROM (
    SELECT
      ace.org_id,
      ace.entity_id AS device_id,
      ace.created_at AS attribute_created_at_ts,
      av.string_value AS attribute_type,
      ROW_NUMBER() OVER(PARTITION BY ace.org_id, ace.entity_id ORDER BY ace.created_at DESC) AS rnk
    FROM attributedb_shards.attribute_cloud_entities AS ace
    LEFT JOIN attributedb_shards.attribute_values AS av 
    ON ace.attribute_value_id = av.uuid
    WHERE av.string_value IN ('Ignition On', 'Reverse Only')
  ) AS sub
  WHERE sub.rnk = 1
),

first_connected AS (
  SELECT
    org_id,
    object_id,
    MIN(date) AS date_baxter_first_connected
  FROM kinesisstats.osdrearcameraconnected
  GROUP BY
    org_id,
    object_id
),

--only joining on devices after 2021-01-01
vg_cpu_usage AS (
  SELECT
    date,
    org_id,
    object_id,
    AVG(value.proto_value.system_stats.total_cpu_util) AS avg_vg_total_cpu_usage
  FROM kinesisstats.osdsystemstats
  WHERE value.is_end = 'false'
    AND value.is_databreak = 'false'
    AND date >= DATE_SUB(${end_date},90)
  GROUP BY
    date,
    org_id,
    object_id
),

cm_cpu_usage AS (
  SELECT
    date,
    org_id,
    object_id,
    AVG(value.proto_value.cm3x_system_stats.total_cpu_util) AS avg_cm_total_cpu_usage
  FROM kinesisstats.osdcm3xsystemstats
  WHERE value.is_end = 'false'
    AND value.is_databreak = 'false'
    AND date >= DATE_SUB(${end_date},90)
  GROUP BY
    date,
    org_id,
    object_id
),

usb_restarts AS (
  SELECT
    date,
    org_id,
    object_id,
    COUNT(*) AS usb_restart_count
  FROM kinesisstats.osdanomalyevent
  WHERE value.is_end = 'false'
    AND value.is_databreak = 'false'
    AND value.proto_value.anomaly_event.service_name LIKE "%camera:firmware.samsaradev.io/dashcam/dashcam.go%"
    AND date >= DATE_SUB(${end_date},90)
  GROUP BY date,
    org_id,
    object_id
),

baxter_health_daily_extended AS (
  SELECT
    a.*,
    att.attribute_created_at_ts,
    att.attribute_type,
    b.avg_vg_total_cpu_usage,
    c.avg_cm_total_cpu_usage,
    d.usb_restart_count,
    e.date_baxter_first_connected
  FROM daily_baxter_health AS a
  LEFT JOIN attributes AS att
    ON a.org_id = att.org_id
    AND a.vg_device_id = att.device_id
  LEFT JOIN vg_cpu_usage AS b
    ON a.date = b.date
    AND a.org_id = b.org_id
    AND a.vg_device_id = b.object_id
  LEFT JOIN cm_cpu_usage AS c
    ON a.date = c.date
    AND a.org_id = c.org_id
    AND a.cm_device_id = c.object_id
  LEFT JOIN usb_restarts AS d
    ON a.date = d.date
    AND a.org_id = d.org_id
    AND a.vg_device_id = d.object_id
  LEFT JOIN first_connected AS e
    ON a.org_id = e.org_id
    AND a.vg_device_id = e.object_id
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
FROM baxter_health_daily_extended
WHERE date >= ${start_date}
  AND date < ${end_date}
