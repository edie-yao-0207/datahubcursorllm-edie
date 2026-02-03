-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Diagnostic Breadth
-- View: Performance by Data Source
-- =============================================================================
-- Visualization: Table
-- Description: Metrics breakdown by signal data source
-- =============================================================================

WITH latest AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'data_source'
)

SELECT
  COALESCE(ds.name, CAST(m.data_source AS STRING)) AS data_source,
  m.monthly.total_trials AS trials,
  m.monthly.graduations AS graduations,
  m.monthly.graduation_rate AS graduation_rate,
  m.monthly.beta_rollback_rate AS beta_rollback_rate
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
LEFT JOIN definitions.promotion_data_source_to_name ds ON ds.id = m.data_source
WHERE m.date = (SELECT max_date FROM latest)
  AND m.grouping_columns = 'data_source'
ORDER BY m.monthly.total_trials DESC
