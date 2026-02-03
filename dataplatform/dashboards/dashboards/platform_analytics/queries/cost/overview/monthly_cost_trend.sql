-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Overview
-- View: Monthly Cost Trend
-- =============================================================================
-- Visualization: Line Chart (multi-series)
-- Title: "Monthly Cost Trend & Budget Status"
-- Description: 30-day rolling cost over time with MoM growth rate. Track monthly
--              spending trajectory against budget thresholds.
--
-- X-Axis: date
-- Y-Axes: 
--   - Primary (left): current_monthly_cost, trailing_12m_cost (USD)
--   - Secondary (right): mom_growth_pct (0-1 scale, format as %)
--
-- Annotations:
--   - Horizontal line at $15,000 on left Y-axis (label: "Budget Upper Limit", color: red)
--   - Horizontal line at $12,000 on left Y-axis (label: "Warning Threshold", color: yellow)
--   - Horizontal line at 0% on right Y-axis (label: "Zero Growth", style: dashed)
-- =============================================================================

SELECT 
  CAST(c.date AS DATE) AS date,
  h.team,
  c.monthly.cost AS current_monthly_cost,
  c.monthly.delta AS month_over_month_change,
  c.monthly.delta / NULLIF(c.monthly.cost - c.monthly.delta, 0) AS mom_growth_pct,
  c.yearly.cost AS trailing_12m_cost,
  CASE 
    WHEN c.monthly.cost > 15000 THEN '🔴 BREACH'  -- Over upper band
    WHEN c.monthly.cost > 12000 THEN '🟡 WARNING'  -- 80-100% of upper band
    ELSE '🟢 OK'
  END AS budget_status
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team'
  AND h.team = :team
  AND c.date BETWEEN :date.min AND :date.max
ORDER BY c.date DESC
