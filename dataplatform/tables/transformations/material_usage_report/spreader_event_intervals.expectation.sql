(
  -- Verify that start and end timestamps make sense
  spreader_start_ms IS NOT NULL
  AND spreader_end_ms IS NOT NULL
  AND spreader_end_ms >= 0
  AND spreader_end_ms >= spreader_start_ms
  AND (mode = 0 || mode = 1)
)
