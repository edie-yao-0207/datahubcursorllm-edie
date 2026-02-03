-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Resource Cost
-- View: Cost Snapshot (KPIs)
-- =============================================================================
-- Visualization: Counter cards
-- Description: Key cost metrics including budget status and efficiency
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
),

latest_cost AS (
  SELECT * FROM cost_data
  ORDER BY date DESC
  LIMIT 1
),

latest_promotions AS (
  SELECT
    monthly.graduations AS monthly_graduations
  FROM product_analytics_staging.agg_signal_promotion_daily_metrics
  WHERE date = (SELECT MAX(date) FROM product_analytics_staging.agg_signal_promotion_daily_metrics WHERE date BETWEEN :date.min AND :date.max)
    AND grouping_columns = 'overall'
)

SELECT
  c.monthly_cost,
  c.monthly_cost - COALESCE(c.prev_monthly_cost, c.monthly_cost) AS mom_change,
  CASE
    WHEN c.monthly_cost > 15000 THEN 'OVER BUDGET'
    WHEN c.monthly_cost > 12000 THEN 'WARNING'
    ELSE 'ON TRACK'
  END AS budget_status,
  -- Cost per signal (rough efficiency metric)
  ROUND(c.monthly_cost / NULLIF(p.monthly_graduations, 0), 2) AS cost_per_graduation
FROM latest_cost c
CROSS JOIN latest_promotions p
