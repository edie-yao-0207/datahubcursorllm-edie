-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Overview
-- View: Cumulative Stats
-- =============================================================================
-- Visualization: Counter cards
-- Description: All-time cumulative promotion statistics
-- =============================================================================

-- Get cumulative stats by summing daily metrics across all time
-- Note: We use daily.* to avoid double-counting from rolling windows
-- Note: All-time unique_signals and unique_populations would require computing
-- COUNT(DISTINCT) across the entire date range, which is not available from
-- pre-aggregated daily metrics.
SELECT
  SUM(m.daily.total_trials) AS all_time_trials,
  SUM(m.daily.graduations) AS all_time_graduations,
  CAST(SUM(m.daily.graduations) AS DOUBLE) / NULLIF(SUM(m.daily.total_trials), 0) AS all_time_graduation_rate,
  SUM(m.daily.beta_rollbacks) AS all_time_beta_rollbacks,
  SUM(m.daily.alpha_rollbacks) AS all_time_alpha_rollbacks
FROM product_analytics_staging.agg_signal_promotion_daily_metrics m
WHERE m.date BETWEEN :date.min AND :date.max
  AND m.grouping_columns = 'overall'

