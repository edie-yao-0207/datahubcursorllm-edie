-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Resource Cost
-- View: Budget Status
-- =============================================================================
-- Visualization: Counter cards
-- Description: Current budget status vs thresholds
-- =============================================================================

WITH cost_data AS (
  SELECT
    c.date,
    c.monthly.cost AS monthly_cost,
    LAG(c.monthly.cost, 30) OVER (ORDER BY c.date) AS prev_monthly_cost
  FROM product_analytics_staging.agg_costs_rolling c
  JOIN product_analytics_staging.dim_cost_hierarchies h USING (date, cost_hierarchy_id)
  WHERE c.date BETWEEN :date.min AND :date.max
    AND h.team = 'firmwarevdp'
    AND h.grouping_columns = 'team'
)

SELECT
  monthly_cost,
  CASE
    WHEN monthly_cost > 15000 THEN 'OVER BUDGET'
    WHEN monthly_cost > 12000 THEN 'WARNING'
    ELSE 'ON TRACK'
  END AS budget_status,
  monthly_cost - COALESCE(prev_monthly_cost, monthly_cost) AS mom_change
FROM cost_data
ORDER BY date DESC
LIMIT 1
