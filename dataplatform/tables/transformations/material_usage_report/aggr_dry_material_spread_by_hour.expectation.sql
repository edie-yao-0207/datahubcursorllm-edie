(
  date IS NOT NULL
  AND org_id IS NOT NULL
  AND device_id IS NOT NULL
  AND interval_start IS NOT NULL
    -- Only output rows that had > 0 material spread via
  -- at least one of the computation methods
  AND (
    (
      material_spread_mg_from_deltas IS NOT NULL
    )
    OR (
      material_spread_mg_from_spread_rate IS NOT NULL
      AND material_spread_mg_from_spread_rate > 0
    )
  )
)
