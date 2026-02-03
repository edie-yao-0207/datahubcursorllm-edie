-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Attribution
-- View: Cost by Product Group & Feature (Detailed)
-- =============================================================================
-- Visualization: Table (sortable, pivot-ready)
-- Title: "Cost Breakdown by Product, Feature, and Service"
-- Description: Detailed cost attribution table showing product_group, 
--              dataplatform_feature, and service combinations over time.
--
-- Columns:
--   - date, product_group, dataplatform_feature, service
--   - monthly_cost, monthly_change, percent_of_total
-- Pivot Options: Rows=product_group, Columns=dataplatform_feature, Values=monthly_cost
-- =============================================================================

SELECT 
  CAST(c.date AS DATE) AS date,
  h.product_group,
  h.dataplatform_feature,
  h.service,
  c.monthly.cost AS monthly_cost,
  c.monthly.delta AS monthly_change,
  c.monthly.cost / SUM(c.monthly.cost) OVER (PARTITION BY c.date) AS percent_of_total
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.service IS NOT NULL  -- Service-level groupings only
  AND h.team = :team
  AND c.date BETWEEN :date.min AND :date.max
  AND h.product_group IS NOT NULL
ORDER BY c.date DESC, monthly_cost DESC
