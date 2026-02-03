org_id IS NOT NULL
AND object_id IS NOT NULL
AND object_type IS NOT NULL
AND interval_start IS NOT NULL
AND gps_distance_m >= gps_distance_personal_m + gps_distance_work_m + gps_distance_commute_m + gps_distance_other_m
