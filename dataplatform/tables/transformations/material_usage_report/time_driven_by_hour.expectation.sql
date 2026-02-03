(
  date IS NOT NULL
  AND org_id IS NOT NULL
  AND device_id IS NOT NULL
  AND hour_start <= hour_end
  AND duration_driven_ms >= 0
)
