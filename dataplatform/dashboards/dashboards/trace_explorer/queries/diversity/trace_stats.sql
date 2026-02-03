-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Trace Diversity
-- View: Trace Stats Counter
-- =============================================================================
-- Visualization: Counter
-- Description: Total traces and unique populations collected from
-- =============================================================================

SELECT
  COUNT(*) AS count_traces,
  COUNT(DISTINCT mmyef_id) AS count_mmyefs
FROM product_analytics_staging.dim_trace_characteristics tc
JOIN product_analytics_staging.dim_device_vehicle_properties dvp USING (date, org_id, device_id)
WHERE tc.date BETWEEN :date.min AND :date.max


