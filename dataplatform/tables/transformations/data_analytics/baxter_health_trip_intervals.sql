WITH osdrearcameraconnected AS ( 
  SELECT
    date,
    org_id,
    object_id AS vg_device_id,
    time AS timestamp,
    value.int_value AS connected
  FROM kinesisstats.osdrearcameraconnected
  WHERE
    value.is_databreak = false AND
    value.is_end = false AND
    date >= DATE_SUB(${end_date}, 14)
),

rear_camera_connected_lag AS (
  SELECT
    vg_device_id,
    org_id,
    COALESCE(LAG(timestamp) OVER (PARTITION BY org_id, vg_device_id ORDER BY timestamp), 0) AS prev_time,
    COALESCE(LAG(connected) OVER (PARTITION BY org_id, vg_device_id ORDER BY timestamp), 0) AS prev_state,
    timestamp AS cur_time,
    connected AS cur_state
  FROM osdrearcameraconnected
),

rear_camera_connected_lead AS (
  SELECT
    vg_device_id,
    org_id,
    timestamp AS prev_time,
    connected AS prev_state,
    COALESCE(LEAD(timestamp) OVER (PARTITION BY org_id, vg_device_id ORDER BY timestamp), 0) AS cur_time,
    COALESCE(LEAD(connected) OVER (PARTITION BY org_id, vg_device_id ORDER BY timestamp), 0) AS cur_state
  FROM osdrearcameraconnected
),

rear_camera_connected_edges AS (
    (
      SELECT *
      FROM rear_camera_connected_lag AS lag
      WHERE lag.prev_state != lag.cur_state
    )
    UNION
    (
      SELECT *
      FROM rear_camera_connected_lead AS lead
      WHERE lead.prev_state != lead.cur_state
    )
),

rear_camera_connected_states AS (
  SELECT
    vg_device_id,
    org_id,
    cur_time AS start_ms,
    cur_state AS connected,
    lead(cur_time) OVER (PARTITION BY org_id, vg_device_id ORDER BY cur_time) AS end_ms
  FROM rear_camera_connected_edges
),

rear_camera_connected_intervals AS (
  SELECT
    org_id,
    vg_device_id,
    start_ms,
    COALESCE(end_ms, unix_timestamp(${end_date}) * 1000) AS end_ms
  FROM rear_camera_connected_states
  WHERE start_ms != 0
    AND connected = 1
),

osdrearcameraframestate AS ( 
  SELECT
    date,
    org_id,
    object_id AS cm3x_device_id,
    time AS timestamp,
    IF(value.int_value = 2, 1, 0) AS is_recording
  FROM kinesisstats.osdrearcameraframestate 
  WHERE value.is_databreak = false
    AND value.is_end = false
    AND date >= DATE_SUB(${end_date}, 14)
    AND object_id IN (SELECT cm_device_id FROM data_analytics.baxter_devices)
),

rear_camera_recording_lag AS (
  SELECT
    cm3x_device_id,
    org_id,
    COALESCE(LAG(timestamp) OVER (PARTITION BY org_id, cm3x_device_id ORDER BY timestamp), 0) AS prev_time,
    COALESCE(LAG(is_recording) OVER (PARTITION BY org_id, cm3x_device_id ORDER BY timestamp), 0) AS prev_state,
    timestamp AS cur_time,
    is_recording AS cur_state
  FROM osdrearcameraframestate
),

rear_camera_recording_lead AS (
  SELECT
    cm3x_device_id,
    org_id,
    timestamp AS prev_time,
    is_recording AS prev_state,
    COALESCE(lead(timestamp) OVER (PARTITION BY org_id, cm3x_device_id ORDER BY timestamp), 0) AS cur_time,
    COALESCE(lead(is_recording) OVER (PARTITION BY org_id, cm3x_device_id ORDER BY timestamp), 0) AS cur_state
  FROM osdrearcameraframestate
),

rear_camera_recording_edges AS (
  (
    SELECT *
    FROM rear_camera_recording_lag AS lag
    WHERE lag.prev_state != lag.cur_state
  )
  UNION
  (
    SELECT *
    FROM rear_camera_recording_lead AS lead
    WHERE lead.prev_state != lead.cur_state
  )
),

rear_camera_recording_states AS (
  SELECT
    cm3x_device_id,
    org_id,
    cur_time AS start_ms,
    cur_state AS is_recording,
    lead(cur_time) OVER (PARTITION BY org_id, cm3x_device_id ORDER BY cur_time) AS end_ms
  FROM rear_camera_recording_edges
),


rear_camera_recording_intervals AS (
  SELECT
    org_id,
    cm3x_device_id,
    start_ms,
    COALESCE(end_ms, unix_timestamp(${end_date}) * 1000) AS end_ms
  FROM rear_camera_recording_states
  WHERE start_ms != 0 and
    is_recording = 1
),

baxter_device_trips AS (
  SELECT cvi.*
  FROM dataprep_safety.cm_vg_intervals AS cvi
  WHERE device_id IN (SELECT vg_device_id FROM data_analytics.baxter_devices)
),

baxter_trip_connected_intervals AS (
   SELECT
    a.date,
    a.org_id,
    a.device_id AS vg_device_id,
    a.cm_device_id,
    a.product_id,
    a.cm_product_id,
    a.start_ms,
    a.end_ms,
    a.duration_ms AS trip_duration_ms,
    a.on_trip,
    SUM(
      CASE WHEN b.end_ms IS NOT NULL AND b.start_ms IS NOT NULL 
        THEN LEAST(b.end_ms, a.end_ms) - greatest(b.start_ms, a.start_ms)
        ELSE 0 
        END
    ) AS connected_duration_ms
  FROM baxter_device_trips AS a
  LEFT JOIN rear_camera_connected_intervals AS b
    ON a.org_id = b.org_id
    AND a.device_id = b.vg_device_id
    AND NOT (b.start_ms >= a.end_ms OR b.end_ms <= a.start_ms)
  WHERE
    a.date >= DATE_SUB(${end_date}, 14) AND
    a.on_trip = true
  GROUP BY
    a.date,
    a.org_id,
    a.device_id,
    a.cm_device_id,
    a.cm_product_id,
    a.product_id,
    a.start_ms,
    a.end_ms,
    a.duration_ms,
    a.on_trip
),

baxter_trip_uptime AS (
  SELECT
    a.date,
    a.org_id,
    a.product_id,
    a.vg_device_id,
    a.cm_device_id,
    a.cm_product_id,
    a.start_ms,
    a.end_ms,
    a.trip_duration_ms,
    a.connected_duration_ms AS baxter_connected_duration_ms,
    GREATEST(MIN(b.start_ms), a.start_ms) AS interval_recording_start_ms,
    LEAST(
      SUM(
        CASE WHEN b.end_ms IS NOT NULL AND b.start_ms IS NOT NULL
            THEN LEAST(b.end_ms, a.end_ms) - greatest(b.start_ms, a.start_ms)
            ELSE 0
            END
      ), 
      a.connected_duration_ms
    ) AS baxter_recording_duration_ms
    FROM baxter_trip_connected_intervals AS a
    LEFT JOIN rear_camera_recording_intervals AS b
      ON a.org_id = b.org_id
      AND a.cm_device_id = b.cm3x_device_id
      AND NOT (b.start_ms >= a.end_ms OR b.end_ms <= a.start_ms)
    GROUP BY
      a.date,
      a.org_id,
      a.vg_device_id,
      a.cm_device_id,
      a.product_id,
      a.cm_product_id,
      a.start_ms,
      a.end_ms,
      a.trip_duration_ms,
      a.connected_duration_ms
),

baxter_recording_durations AS (
  SELECT 
    org_id,
    vg_device_id,
    cm_device_id,
    product_id,
    cm_product_id,
    start_ms,
    end_ms,
    trip_duration_ms,
    date,
    baxter_connected_duration_ms,
    CASE WHEN interval_recording_start_ms < start_ms + 90*1000 THEN baxter_recording_duration_ms + interval_recording_start_ms - start_ms
         ELSE baxter_recording_duration_ms
         END AS baxter_grace_recording_duration_ms,
    baxter_recording_duration_ms
  FROM baxter_trip_uptime
),

cm_device_health_intervals AS (
  SELECT
    a.*,
    b.cm_total_anomalies,
    b.cpu_gold_max_temp_celsius AS cm_cpu_gold_max_temp_celsius,
    b.cpu_gold_avg_temp_celsius AS cm_cpu_gold_avg_temp_celsius,
    b.overheated_count AS cm_overheated_count,
    b.safe_mode_count AS cm_safe_mode_count
  FROM cm_health_report.cm_recording_intervals AS a
  LEFT JOIN dataprep_safety.cm_device_health_intervals AS b
    ON a.org_id = b.org_id
    AND a.device_id = b.device_id
    AND a.cm_device_id = b.cm_device_id
    AND a.date = b.date
    AND a.start_ms = b.start_ms
    AND a.end_ms = b.end_ms
  WHERE 
    a.date >= DATE_SUB(${end_date}, 14)
    AND a.cm_device_id IN (SELECT cm_device_id FROM data_analytics.baxter_devices)
),

baxter_health_trip_intervals AS (
  SELECT
    a.*,
    b.cm_bootcount,
    b.cm_total_anomalies,
    b.cm_cpu_gold_max_temp_celsius,
    b.cm_cpu_gold_avg_temp_celsius,
    b.cm_overheated_count,
    b.cm_safe_mode_count,
    b.cm_build,
    b.vg_build,
    b.interval_connected_duration_ms AS cm_connected_duration_ms,
    b.grace_recording_duration_ms AS cm_recording_duration_ms,
    ROUND(100 * a.baxter_connected_duration_ms / a.trip_duration_ms,2) AS baxter_trip_connected_uptime_percentage,
    ROUND(100 * a.baxter_recording_duration_ms / a.trip_duration_ms,2) AS baxter_trip_recording_uptime_percentage,
    ROUND(100 * a.baxter_grace_recording_duration_ms / a.trip_duration_ms,2) as baxter_trip_grace_recording_uptime_percentage,
    ROUND(100 * b.interval_connected_duration_ms / b.duration_ms,2) AS cm3x_trip_connected_uptime_percentage,
    ROUND(100 * b.grace_recording_duration_ms / b.duration_ms,2) AS cm3x_trip_recording_uptime_percentage
  FROM baxter_recording_durations AS a
  LEFT JOIN cm_device_health_intervals AS b
    ON a.org_id = b.org_id
    AND a.vg_device_id = b.device_id
    AND a.cm_device_id = b.cm_device_id
    AND a.date = b.date
    AND a.start_ms = b.start_ms
    AND a.end_ms = b.end_ms
)

SELECT
    date,
    org_id,
    product_id,
    vg_device_id,
    vg_build,
    cm_product_id,
    cm_device_id,
    cm_build,
    start_ms,
    end_ms,
    trip_duration_ms,
    baxter_connected_duration_ms,
    baxter_grace_recording_duration_ms,
    baxter_recording_duration_ms,
    cm_bootcount,
    cm_total_anomalies,
    cm_cpu_gold_max_temp_celsius,
    cm_cpu_gold_avg_temp_celsius,
    cm_overheated_count,
    cm_safe_mode_count,
    cm_connected_duration_ms,
    cm_recording_duration_ms,
    baxter_trip_connected_uptime_percentage,
    baxter_trip_recording_uptime_percentage,
    baxter_trip_grace_recording_uptime_percentage,
    cm3x_trip_connected_uptime_percentage,
    cm3x_trip_recording_uptime_percentage
FROM baxter_health_trip_intervals
WHERE date >= ${start_date}
  AND date < ${end_date}
