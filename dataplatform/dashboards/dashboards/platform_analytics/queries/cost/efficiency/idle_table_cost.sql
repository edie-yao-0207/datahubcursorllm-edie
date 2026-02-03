-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Efficiency
-- View: Idle Service Costs
-- =============================================================================
-- Visualization: Bar Chart + Table
-- Title: "Idle & Low-Usage Services (>$10/month)"
-- Description: Services costing >$10/month with <3 users. Deprecation candidates.
--              Target SLO: Idle cost < 5% of total team budget.
--
-- Chart: Horizontal bar of monthly_cost by service (top 20)
-- Table Columns:
--   - date, team, service
--   - monthly_cost, monthly_users, monthly_queries
--   - usage_category (ZERO_USAGE, LOW_USAGE, USED)
-- =============================================================================

-- Look at last 30 days and take most recent metrics for each service
WITH recent_costs AS (
  SELECT 
    c.date,
    h.region,
    h.team,
    h.service,
    c.monthly.cost AS monthly_cost,
    c.cost_hierarchy_id,
    ROW_NUMBER() OVER (PARTITION BY h.service ORDER BY c.date DESC) AS rn
  FROM product_analytics_staging.agg_costs_rolling c
  JOIN product_analytics_staging.dim_cost_hierarchies h
    USING (date, cost_hierarchy_id)
  WHERE h.grouping_columns = 'region.team.product_group.dataplatform_feature.service'
    AND h.team = :team
    -- Look at last 30 days to capture all recent services
    AND c.date >= DATE_SUB((SELECT MAX(date) FROM product_analytics_staging.agg_costs_rolling WHERE date BETWEEN :date.min AND :date.max), 29)
),
recent_usage AS (
  SELECT 
    u.cost_hierarchy_id,
    u.date,
    u.monthly.unique_users AS monthly_users,
    u.monthly.total_queries AS monthly_queries,
    ROW_NUMBER() OVER (PARTITION BY u.cost_hierarchy_id ORDER BY u.date DESC) AS rn
  FROM product_analytics_staging.agg_table_usage_rolling u
  JOIN product_analytics_staging.dim_cost_hierarchies h
    USING (date, cost_hierarchy_id)
  WHERE h.grouping_columns = 'region.team.product_group.dataplatform_feature.service'
    AND h.team = :team
    AND u.date >= DATE_SUB((SELECT MAX(date) FROM product_analytics_staging.agg_table_usage_rolling WHERE date BETWEEN :date.min AND :date.max), 29)
)
SELECT 
  CAST(c.date AS DATE) AS date,
  c.region,
  c.team,
  c.service,
  c.monthly_cost,
  COALESCE(u.monthly_users, 0) AS monthly_users,
  COALESCE(u.monthly_queries, 0) AS monthly_queries,
  CASE 
    WHEN COALESCE(u.monthly_users, 0) = 0 THEN 'ZERO_USAGE'
    WHEN COALESCE(u.monthly_users, 0) < 3 THEN 'LOW_USAGE'
    ELSE 'USED'
  END AS usage_category,
  -- Quick navigation links (service = database.table format)
  CONCAT('https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,', c.service, ',PROD)/Schema') AS datahub_url,
  CONCAT('https://dagster.internal.samsara.com/assets/', c.region, '/', REPLACE(c.service, '.', '/')) AS dagster_url
FROM recent_costs c
LEFT JOIN recent_usage u
  ON c.cost_hierarchy_id = u.cost_hierarchy_id
  AND u.rn = 1
WHERE c.rn = 1  -- Take most recent row for each service
  AND c.monthly_cost > 10  -- Only show services with material cost
  AND COALESCE(u.monthly_users, 0) < 3  -- Low or zero usage
ORDER BY c.monthly_cost DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
