-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Resource Cost
-- View: Cost by Product Group
-- =============================================================================
-- Visualization: Stacked Area Chart
-- Description: Cost breakdown by product group over time
-- =============================================================================

SELECT
  CAST(c.date AS DATE) AS date,
  h.product_group,
  c.monthly.cost AS monthly_cost
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h USING (date, cost_hierarchy_id)
WHERE c.date BETWEEN :date.min AND :date.max
  AND h.team = 'firmwarevdp'
  AND h.grouping_columns = 'team.product_group'
ORDER BY c.date, h.product_group
