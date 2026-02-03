start_ms < end_ms
AND org_id IS NOT NULL
AND device_id IS NOT NULL
AND start_ms IS NOT NULL
AND end_ms IS NOT NULL
AND date IS NOT NULL
AND end_ms - start_ms = duration_ms
AND interval_connected_duration_ms >= 0
AND interval_connected_duration_ms <= duration_ms
AND grace_recording_duration_ms >= 0
AND grace_recording_duration_ms <= duration_ms
AND cm_product_id IN (30, 31) -- only cm2x devices
