(cm_product_id IN (30, 31) AND linked_cm_id IS NULL)
OR
(cm_product_id IN (43, 44, 167, 155) AND linked_cm_id IS NOT NULL)
AND org_id IS NOT NULL
AND vg_device_id IS NOT NULL
AND product_id IS NOT NULL
AND cm_product_id IS NOT NULL
AND upper_camera_serial IS NOT NULL AND upper_camera_serial != ""
