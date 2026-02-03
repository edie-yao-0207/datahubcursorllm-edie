-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Trace Diversity
-- View: Vehicle Trace Data Distribution Analysis
-- =============================================================================
-- Visualization: Histogram
-- Description: Trace count distribution per MMYEF population
-- =============================================================================

SELECT
  d.mmyef_id,
  COUNT(DISTINCT t.trace_uuid) AS trace_cnt
FROM product_analytics_staging.dim_trace_characteristics t
JOIN product_analytics_staging.dim_device_vehicle_properties d
  USING (date, org_id, device_id)
WHERE t.date BETWEEN :date.min AND :date.max
GROUP BY d.mmyef_id


