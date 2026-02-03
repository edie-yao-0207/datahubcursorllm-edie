-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Attribution
-- View: Top Services by Cost
-- =============================================================================
-- Visualization: Bar Chart (horizontal) + Table
-- Title: "Top Services by Cost"
-- Description: Identify the highest-cost services for optimization. Shows monthly 
--              cost, recent trend, and percent of total FirmwareVDP spending.
--
-- Parameters:
--   :limit (Number) - Max rows to return (default: 50, use 0 for no limit)
--
-- Chart: Horizontal bar chart of monthly_cost by service
-- Table Columns:
--   - date, region, team, service, product_group, dataplatform_feature
--   - monthly_cost, monthly_change, yesterday_cost, percent_of_total
-- Sort: monthly_cost DESC
-- =============================================================================

-- Look at last 30 days and take most recent metrics for each service
WITH recent_costs AS (
  SELECT 
    c.date,
    h.region,
    h.team,
    h.service,
    h.product_group,
    h.dataplatform_feature,
    c.monthly.cost AS monthly_cost,
    c.monthly.delta AS monthly_change,
    c.daily.cost AS yesterday_cost,
    ROW_NUMBER() OVER (PARTITION BY h.service ORDER BY c.date DESC) AS rn
  FROM product_analytics_staging.agg_costs_rolling c
  JOIN product_analytics_staging.dim_cost_hierarchies h
    USING (date, cost_hierarchy_id)
  WHERE h.service IS NOT NULL  -- Service-level groupings only
    AND h.team = :team
    -- Look at last 30 days to capture all recent services
    AND c.date >= DATE_SUB((SELECT MAX(date) FROM product_analytics_staging.agg_costs_rolling WHERE date BETWEEN :date.min AND :date.max), 29)
)
SELECT 
  CAST(date AS DATE) AS date,
  region,
  team,
  service,
  product_group,
  dataplatform_feature,
  monthly_cost,
  monthly_change,
  yesterday_cost,
  monthly_cost / SUM(monthly_cost) OVER () AS percent_of_total,
  -- Quick navigation links (service = database.table format)
  CONCAT('https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,', service, ',PROD)/Schema') AS datahub_url,
  CONCAT('https://dagster.internal.samsara.com/assets/', region, '/', REPLACE(service, '.', '/')) AS dagster_url
FROM recent_costs
WHERE rn = 1  -- Take most recent row for each service
ORDER BY monthly_cost DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
