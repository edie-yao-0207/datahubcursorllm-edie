-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Resource Cost
-- View: Platform Cost vs Budget
-- =============================================================================
-- Visualization: Line Chart with reference lines
-- Description: Monthly rolling cost trend with budget thresholds
-- =============================================================================

SELECT
  CAST(c.date AS DATE) AS date,
  c.monthly.cost AS monthly_cost
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h USING (date, cost_hierarchy_id)
WHERE c.date BETWEEN :date.min AND :date.max
  AND h.team = 'firmwarevdp'
  AND h.grouping_columns = 'team'
ORDER BY c.date
