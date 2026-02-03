org_id IS NOT NULL
AND device_id IS NOT NULL
AND ((cm_product_id IN (30, 31) AND cm_device_id IS NULL) OR (cm_product_id IN (43, 44, 167, 155) AND cm_device_id IS NOT NULL))
AND cm_product_id IS NOT NULL
AND upper_camera_serial IS NOT NULL
AND upper_camera_serial <> ""
AND s1_recording_ms >= 0
AND s1_uptime_pct >= 0
AND s1_connected_ms <= s1_trip_ms
AND s2_recording_ms >= 0
AND s2_uptime_pct >= 0
AND s2_connected_ms <= s2_trip_ms
AND s3_recording_ms >= 0
AND s3_uptime_pct >= 0
AND s3_connected_ms <= s3_trip_ms
AND s4_recording_ms >= 0
AND s4_uptime_pct >= 0
AND s4_connected_ms <= s4_trip_ms
AND s5_recording_ms >= 0
AND s5_uptime_pct >= 0
AND s5_connected_ms <= s5_trip_ms
AND overall_recording_ms >= 0
AND overall_uptime_pct >= 0
AND overall_connected_ms <= overall_trip_ms
