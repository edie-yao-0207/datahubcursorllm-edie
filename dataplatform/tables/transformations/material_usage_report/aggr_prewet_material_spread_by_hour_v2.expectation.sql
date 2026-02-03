(
  date IS NOT NULL
  AND org_id IS NOT NULL
  AND device_id IS NOT NULL
  AND interval_start IS NOT NULL
  -- Only output rows that had > 0 prewet material spread via
  -- at least one of the computation methods
  AND (
    (
      material_spread_ml_from_deltas is NOT NULL
    ) OR (
      material_spread_ml_from_mix_rate is NOT NULL AND material_spread_ml_from_mix_rate > 0
    ) OR (
      material_spread_ml_from_spread_rate is NOT NULL AND material_spread_ml_from_spread_rate > 0
    )
  )
)
