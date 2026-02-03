-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Resource Cost
-- View: Cost Efficiency Trend
-- =============================================================================
-- Visualization: Line Chart
-- Description: Cost per graduated signal over time
-- =============================================================================

WITH costs AS (
  SELECT
    c.date,
    c.monthly.cost AS monthly_cost
  FROM product_analytics_staging.agg_costs_rolling c
  JOIN product_analytics_staging.dim_cost_hierarchies h USING (date, cost_hierarchy_id)
  WHERE c.date BETWEEN :date.min AND :date.max
    AND h.team = 'firmwarevdp'
    AND h.grouping_columns = 'team'
),

promotions AS (
  SELECT
    date,
    monthly.graduations AS monthly_graduations
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'overall'
)

SELECT
  CAST(c.date AS DATE) AS date,
  c.monthly_cost,
  p.monthly_graduations,
  ROUND(c.monthly_cost / NULLIF(p.monthly_graduations, 0), 2) AS cost_per_graduation
FROM costs c
JOIN promotions p ON c.date = p.date
ORDER BY c.date
