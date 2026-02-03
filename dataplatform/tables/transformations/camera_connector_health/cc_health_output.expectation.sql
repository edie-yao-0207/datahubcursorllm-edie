org_id IS NOT NULL
AND vg_id IS NOT NULL
AND cm_id IS NOT NULL
AND s1_recording_ms >= 0
AND s1_recording_uptime_pct >= 0
AND s1_recording_ms <= s1_connected_ms
AND s1_connected_ms <= s1_trip_ms
AND s2_recording_ms >= 0
AND s2_recording_uptime_pct >= 0
AND s2_recording_ms <= s2_connected_ms
AND s2_connected_ms <= s2_trip_ms
AND s3_recording_ms >= 0
AND s3_recording_uptime_pct >= 0
AND s3_recording_ms <= s3_connected_ms
AND s3_connected_ms <= s3_trip_ms
AND s4_recording_ms >= 0
AND s4_recording_uptime_pct >= 0
AND s4_recording_ms <= s4_connected_ms
AND s4_connected_ms <= s4_trip_ms
AND s5_recording_ms >= 0
AND s5_recording_uptime_pct >= 0
AND s5_recording_ms <= s5_connected_ms
AND s5_connected_ms <= s5_trip_ms
AND overall_recording_ms >= 0
AND overall_recording_uptime_pct >= 0
AND overall_recording_ms <= overall_connected_ms
AND overall_connected_ms <= overall_trip_ms
AND s1_trip_ms + s2_trip_ms + s3_trip_ms + s4_trip_ms + s5_trip_ms = overall_trip_ms
AND s1_connected_ms + s2_connected_ms + s3_connected_ms + s4_connected_ms + s5_connected_ms = overall_connected_ms
AND s1_recording_ms + s2_recording_ms + s3_recording_ms + s4_recording_ms + s5_recording_ms = overall_recording_ms
