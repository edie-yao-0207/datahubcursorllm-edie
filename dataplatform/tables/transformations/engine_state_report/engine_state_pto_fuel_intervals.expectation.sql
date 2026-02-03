NOT (
  fuel_consumed_ml < 3785.41 -- 1 gallon
  AND(
    original_engine_state_interval_duration_ms >= 86400000 -- 24 hours
    OR original_fuel_interval_duration_ms >= 86400000
    OR original_pto_interval_duration_ms >= 86400000
  )
  AND engine_state in ("ON", "IDLE")
)
