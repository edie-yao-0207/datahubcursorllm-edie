-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Diagnostic Breadth
-- View: Signals by Data Source
-- =============================================================================
-- Visualization: Bar Chart
-- Description: Distinct signals promoted by data source
-- =============================================================================

WITH latest AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'data_source'
)

SELECT
  COALESCE(ds.name, CAST(m.data_source AS STRING)) AS data_source,
  m.daily.unique_signals AS unique_signals,
  m.monthly.graduations AS graduations
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
LEFT JOIN definitions.promotion_data_source_to_name ds ON ds.id = m.data_source
WHERE m.date = (SELECT max_date FROM latest)
  AND m.grouping_columns = 'data_source'
ORDER BY m.monthly.graduations DESC
