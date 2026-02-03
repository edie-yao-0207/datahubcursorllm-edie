-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Monthly Collection Growth
-- =============================================================================
-- Visualization: Bar chart
-- Description: Monthly collection growth (last 6 months)
-- =============================================================================

SELECT
  DATE_TRUNC('month', date) AS collection_month,
  COUNT(DISTINCT trace_uuid) AS traces_collected,
  COUNT(DISTINCT device_id) AS devices_with_traces
FROM product_analytics_staging.fct_can_trace_status
WHERE date BETWEEN :date.min AND :date.max
  AND is_available = TRUE
GROUP BY DATE_TRUNC('month', date)
ORDER BY collection_month DESC
LIMIT 6

