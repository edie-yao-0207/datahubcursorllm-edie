-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Overview
-- View: Cost by Time Window
-- =============================================================================
-- Visualization: Line Chart (multi-series)
-- Title: "Cost Across Time Horizons"
-- Description: Compare daily, weekly (7d), monthly (30d), quarterly (90d), and
--              yearly (365d) rolling costs to understand spend patterns.
--
-- X-Axis: date
-- Y-Axis (left): daily_cost, rolling_7d_cost, rolling_30d_cost, rolling_90d_cost, rolling_365d_cost (USD)
-- Y-Axis (right): daily_change, weekly_change, monthly_change, yearly_change (USD)
-- Series: One line per time window
--
-- Annotations:
--   - Horizontal line at $0 on right Y-axis (label: "Zero Change", style: dashed)
--   - Horizontal line at $500/day on daily_cost (label: "Daily Budget", color: yellow)
--   - Horizontal line at $12,000 on rolling_30d_cost (label: "Monthly Budget", color: yellow)
-- =============================================================================

SELECT 
  CAST(c.date AS DATE) AS date,
  c.daily.cost AS daily_cost,
  c.weekly.cost AS rolling_7d_cost,
  c.monthly.cost AS rolling_30d_cost,
  c.quarterly.cost AS rolling_90d_cost,
  c.yearly.cost AS rolling_365d_cost,
  c.daily.delta AS daily_change,
  c.weekly.delta AS weekly_change,
  c.monthly.delta AS monthly_change,
  c.yearly.delta AS yearly_change
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team'
  AND h.team = :team
  AND c.date BETWEEN :date.min AND :date.max
ORDER BY c.date DESC
