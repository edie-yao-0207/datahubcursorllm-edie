start_ms < end_ms
AND org_id IS NOT NULL
AND vg_device_id IS NOT NULL
AND start_ms IS NOT NULL
AND end_ms IS NOT NULL
AND date IS NOT NULL
AND end_ms - start_ms = duration_ms
AND interval_connected_duration_ms >= 0
AND interval_connected_duration_ms <= duration_ms
AND grace_recording_duration_ms >= 0
AND grace_recording_duration_ms <= duration_ms
AND cm_product_id IN (43, 44, 167, 155) -- only cm3x devices
