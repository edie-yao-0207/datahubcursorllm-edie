-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Diagnostic Depth
-- View: Coverage Rate Trend by Signal
-- =============================================================================
-- Visualization: Line Chart
-- Description: Daily coverage trend by signal type over time
-- =============================================================================

WITH daily_coverage AS (
  SELECT
    c.date,
    c.type,
    -- Aggregate across all market groupings to get overall coverage
    SUM(c.coverage_count_distinct_device_id) AS total_covered,
    SUM(c.population_count_distinct_device_id) AS total_population
  FROM product_analytics_staging.agg_telematics_actual_coverage_normalized c
  WHERE c.date BETWEEN :date.min AND :date.max
  GROUP BY c.date, c.type
)

SELECT
  CAST(d.date AS DATE) AS date,
  m.signal_name,
  COALESCE(d.total_covered / NULLIF(d.total_population, 0), 0) AS coverage_rate
FROM daily_coverage d
JOIN product_analytics_staging.fct_telematics_stat_metadata m ON d.type = m.type
ORDER BY d.date, m.signal_name
