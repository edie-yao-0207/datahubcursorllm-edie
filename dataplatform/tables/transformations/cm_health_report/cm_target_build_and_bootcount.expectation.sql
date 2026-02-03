start_ms < end_ms
AND org_id IS NOT NULL
AND vg_device_id IS NOT NULL
AND start_ms IS NOT NULL
AND end_ms IS NOT NULL
AND vg_bootcount >= 0
AND cm_bootcount >= 0
AND ((cm_product_id IN (30, 31) AND cm_device_id IS NULL) OR (cm_product_id IN (43, 44, 167, 155) AND cm_device_id IS NOT NULL))
