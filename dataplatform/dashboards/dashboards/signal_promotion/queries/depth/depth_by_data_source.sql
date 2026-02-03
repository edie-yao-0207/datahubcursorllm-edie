-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Depth
-- View: Depth by Data Source
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Signal and population diversity breakdown by data source
-- =============================================================================

WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'data_source'
)

SELECT
  COALESCE(ds.name, CAST(m.data_source AS STRING)) AS data_source,
  m.data_source AS data_source_id,
  -- Daily unique counts (rolling window unique counts not available)
  MAX(m.daily.unique_signals) AS unique_signals,
  MAX(m.daily.unique_populations) AS unique_populations,
  MAX(m.monthly.total_trials) AS total_trials,
  MAX(m.monthly.graduation_rate) AS graduation_rate
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
LEFT JOIN definitions.promotion_data_source_to_name ds ON ds.id = m.data_source
WHERE m.date = (SELECT max_date FROM latest_date)
  AND m.grouping_columns = 'data_source'
  AND m.data_source IS NOT NULL
GROUP BY ds.name, m.data_source
ORDER BY unique_signals DESC

