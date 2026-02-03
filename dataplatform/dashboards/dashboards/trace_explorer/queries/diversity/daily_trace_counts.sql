-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Trace Diversity
-- View: Traces Pulled Per Day
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Daily trace counts
-- =============================================================================

SELECT
  DATE(tc.date) AS date,
  COUNT(*) AS count_traces
FROM product_analytics_staging.dim_trace_characteristics tc
WHERE tc.date BETWEEN :date.min AND :date.max
GROUP BY DATE(tc.date)
ORDER BY date


