-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage - Tables
-- View: Most Accessed Services
-- =============================================================================
-- Visualization: Bar Chart (horizontal) + Table
-- Title: "Top 50 Most Accessed Services"
-- Description: Services with highest unique user count. These are your most 
--              valuable data products - prioritize their quality and SLAs.
--
-- Chart: Horizontal bar of monthly_users by service (top 20)
-- Table Columns:
--   - date, service, product_group, dataplatform_feature
--   - monthly_users, monthly_queries, avg_queries_per_user, percent_of_users
-- Sort: monthly_users DESC
-- =============================================================================

-- Look at last 30 days and take most recent metrics for each service
WITH recent_activity AS (
  SELECT 
    u.date,
    h.region,
    h.service,
    h.product_group,
    h.dataplatform_feature,
    u.monthly.unique_users AS monthly_users,
    u.monthly.total_queries AS monthly_queries,
    u.monthly.avg_queries_per_user AS avg_queries_per_user,
    ROW_NUMBER() OVER (PARTITION BY h.service ORDER BY u.date DESC) AS rn
  FROM product_analytics_staging.agg_table_usage_rolling u
  JOIN product_analytics_staging.dim_cost_hierarchies h
    USING (date, cost_hierarchy_id)
  WHERE h.service IS NOT NULL  -- Service-level groupings only
    AND h.team = :team
    -- Look at last 30 days to capture all recently accessed services
    AND u.date >= DATE_SUB((SELECT MAX(date) FROM product_analytics_staging.agg_table_usage_rolling WHERE date BETWEEN :date.min AND :date.max), 29)
)
SELECT 
  CAST(date AS DATE) AS date,
  region,
  service,
  product_group,
  dataplatform_feature,
  monthly_users,
  monthly_queries,
  avg_queries_per_user,
  monthly_users / SUM(monthly_users) OVER () AS percent_of_users,
  -- Quick navigation links (service = database.table format)
  CONCAT('https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,', service, ',PROD)/Schema') AS datahub_url,
  CONCAT('https://dagster.internal.samsara.com/assets/', region, '/', REPLACE(service, '.', '/')) AS dagster_url
FROM recent_activity
WHERE rn = 1  -- Take most recent row for each service
ORDER BY monthly_users DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
