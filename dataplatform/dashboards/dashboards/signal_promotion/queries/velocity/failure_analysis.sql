-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Velocity
-- View: Rollback Analysis
-- =============================================================================
-- Visualization: Line Chart
-- Description: Rollback rates by type (beta rollbacks = production issues, alpha = expected churn)
-- =============================================================================

SELECT
  CAST(m.date AS DATE) AS date,
  m.monthly.beta_rollback_rate AS beta_rollback_rate,
  m.monthly.alpha_rollback_rate AS alpha_rollback_rate,
  m.monthly.in_progress_rate AS in_progress_rate
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
WHERE m.date BETWEEN :date.min AND :date.max
  AND m.grouping_columns = 'overall'
ORDER BY m.date DESC

