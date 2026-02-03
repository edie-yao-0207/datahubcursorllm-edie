-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Overview
-- View: Top Cost Summary
-- =============================================================================
-- Visualization: Counter (4 separate counters in a row)
-- Title: "FirmwareVDP Platform Cost Summary"
-- Description: Executive-level KPIs showing current month costs, growth trends, 
--              and critical anomaly count. Updates daily.
--
-- Counters:
--   1. Total Monthly Cost (USD)
--   2. Month-over-Month Growth (USD)
--   3. MoM Growth % (%)
--   4. Critical Anomalies (count)
-- =============================================================================

WITH latest_date AS (
  SELECT MAX(c.date) AS max_date
  FROM product_analytics_staging.agg_costs_rolling c
  WHERE c.date BETWEEN :date.min AND :date.max
)

SELECT 
  'Total Monthly Cost' AS metric,
  MAX(c.monthly.cost) AS value,
  'USD' AS unit
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team'
  AND h.team = :team
  AND c.date = DATE_SUB((SELECT max_date FROM latest_date), 1)

UNION ALL

SELECT 
  'Month-over-Month Growth' AS metric,
  MAX(c.monthly.delta) AS value,
  'USD' AS unit
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team'
  AND h.team = :team
  AND c.date = DATE_SUB((SELECT max_date FROM latest_date), 1)

UNION ALL

SELECT 
  'MoM Growth %' AS metric,
  MAX(c.monthly.delta) / NULLIF(MAX(c.monthly.cost - c.monthly.delta), 0) AS value,
  '%' AS unit
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team'
  AND h.team = :team
  AND c.date = DATE_SUB((SELECT max_date FROM latest_date), 1)

UNION ALL

SELECT 
  'Critical Anomalies' AS metric,
  COUNT(*) AS value,
  'services' AS unit
FROM product_analytics_staging.fct_table_cost_anomalies a
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'region.team.product_group.dataplatform_feature.service'
  AND h.team = :team
  AND a.date = DATE_SUB((SELECT max_date FROM latest_date), 1)
  AND a.daily.status IN ('critical_increase', 'critical_decrease')
