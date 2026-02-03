-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Attribution
-- View: Cost by Product Group
-- =============================================================================
-- Visualization: Stacked Area Chart
-- Title: "Monthly Cost by Product Group & Feature"
-- Description: Time-series view of how costs are distributed across product 
--              groups and dataplatform features within FirmwareVDP.
--
-- X-Axis: date
-- Y-Axis: monthly_cost (USD, stacked)
-- Stack by: product_group, then dataplatform_feature
-- Tooltip: monthly_change, percent_of_firmwarevdp_total
--
-- Annotations:
--   - Horizontal line at $12,000 total (label: "Monthly Budget", color: yellow)
--   - Horizontal line at $15,000 total (label: "Budget Limit", color: red)
--   - Stacked total line overlay to show overall trend vs budget
-- =============================================================================

SELECT 
  CAST(c.date AS DATE) AS date,
  h.product_group,
  c.monthly.cost AS monthly_cost,
  c.monthly.delta AS monthly_change,
  c.monthly.cost / SUM(c.monthly.cost) OVER (PARTITION BY c.date) AS percent_of_firmwarevdp_total
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team.product_group'
  AND h.team = :team
  AND c.date BETWEEN :date.min AND :date.max
  AND h.product_group IS NOT NULL
ORDER BY c.date DESC, monthly_cost DESC
