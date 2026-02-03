-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Depth
-- View: Volume by Data Source Over Time
-- =============================================================================
-- Visualization: Line Chart
-- Description: Promotion volume trends by data source over time
-- =============================================================================

SELECT
  CAST(m.date AS DATE) AS date,
  COALESCE(ds.name, CAST(m.data_source AS STRING)) AS data_source,
  m.monthly.total_trials AS monthly_trials,
  -- Daily unique signals (rolling window unique counts not available)
  m.daily.unique_signals AS daily_signals,
  m.monthly.graduation_rate AS monthly_graduation_rate
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
LEFT JOIN definitions.promotion_data_source_to_name ds ON ds.id = m.data_source
WHERE m.date BETWEEN :date.min AND :date.max
  AND m.grouping_columns = 'data_source'
  AND m.data_source IS NOT NULL
ORDER BY m.date, ds.name

