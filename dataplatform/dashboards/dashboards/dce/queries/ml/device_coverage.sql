-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: ML Model Metrics
-- View: Device Coverage Trend
-- =============================================================================
-- Visualization: Line Chart
-- Description: Number of promoted and promotable devices over time
-- =============================================================================

SELECT
  date,
  signal,
  SUM(promoted_device_count) AS promoted_device_count,
  SUM(promoted_and_promotable_device_count) AS promoted_and_promotable_device_count
FROM default.dojo.signal_reverse_engineering_metrics_v0
WHERE date BETWEEN :date.min AND :date.max
GROUP BY date, signal
ORDER BY date

