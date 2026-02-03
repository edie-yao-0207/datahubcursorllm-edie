-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Diagnostic Depth
-- View: Coverage Rate Trend by Signal
-- =============================================================================
-- Visualization: Line Chart
-- Description: Daily coverage trend by signal type over time
-- =============================================================================

SELECT
  CAST(c.date AS DATE) AS date,
  m.signal_name,
  c.percent_coverage AS coverage_rate
FROM product_analytics_staging.agg_telematics_actual_coverage_normalized c
JOIN product_analytics_staging.fct_telematics_stat_metadata m USING (type)
WHERE c.date BETWEEN :date.min AND :date.max
  AND c.grouping_hash = 'overall'  -- Overall coverage, not by market/segment
ORDER BY c.date, m.signal_name
