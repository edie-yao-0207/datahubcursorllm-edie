-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: Resource Cost
-- View: Top 10 Cost Drivers
-- =============================================================================
-- Visualization: Table
-- Description: Highest cost tables/services
-- =============================================================================

WITH latest AS (
  SELECT MAX(c.date) AS max_date
  FROM product_analytics_staging.agg_costs_rolling c
  JOIN product_analytics_staging.dim_cost_hierarchies h USING (date, cost_hierarchy_id)
  WHERE c.date BETWEEN :date.min AND :date.max
    AND h.team = 'firmwarevdp'
    AND h.grouping_columns = 'region.team.product_group.dataplatform_feature.service'
),

total AS (
  SELECT SUM(c.monthly.cost) AS total_cost
  FROM product_analytics_staging.agg_costs_rolling c
  JOIN product_analytics_staging.dim_cost_hierarchies h USING (date, cost_hierarchy_id)
  WHERE c.date = (SELECT max_date FROM latest)
    AND h.team = 'firmwarevdp'
    AND h.grouping_columns = 'region.team.product_group.dataplatform_feature.service'
)

SELECT
  COALESCE(h.service, h.dataplatform_feature, h.product_group) AS cost_driver,
  h.product_group,
  c.monthly.cost AS monthly_cost,
  ROUND(c.monthly.cost / NULLIF(t.total_cost, 0), 3) AS percent_of_total
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h USING (date, cost_hierarchy_id)
CROSS JOIN total t
WHERE c.date = (SELECT max_date FROM latest)
  AND h.team = 'firmwarevdp'
  AND h.grouping_columns = 'region.team.product_group.dataplatform_feature.service'
ORDER BY c.monthly.cost DESC
LIMIT 10
