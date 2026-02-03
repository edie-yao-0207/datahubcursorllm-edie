(
  -- Verify that start and end timestamps make sense
  spreader_start_ms <= spreader_end_ms
  AND spread_rate_start_ms <= spread_rate_end_ms
  AND spread_rate_event_start_ms <= spread_rate_event_end_ms
  AND spread_rate > 0
  AND distance_covered_meters IS NOT NULL
  AND distance_covered_meters >= 0
)
