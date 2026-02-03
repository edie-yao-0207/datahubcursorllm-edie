-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage Overview
-- View: Usage Snapshot
-- =============================================================================
-- Visualization: Scorecards (4-6 cards)
-- Title: "Usage At-a-Glance"
-- Purpose: Quick health check - how are users engaging with our data?
--
-- Scorecards:
--   1. "Monthly Active Users" - monthly_active_users (count)
--   2. "Monthly Queries" - monthly_queries (count, formatted as K/M)
--   3. "Tables Accessed" - unique_tables_monthly (count)
--   4. "Queries/User" - avg_queries_per_user (ratio)
--   5. "User Growth" - user_growth_pct (%, green if positive)
--   6. "Query Growth" - query_growth_pct (%, green if positive)
--
-- Annotations:
--   - Show delta vs previous period
--   - Color code growth metrics (green positive, red negative)
-- =============================================================================

-- Latest day snapshot for scorecards
WITH latest_data AS (
  SELECT 
    u.date,
    u.monthly.unique_users AS monthly_active_users,
    u.monthly.total_queries AS monthly_queries,
    u.monthly.unique_tables AS unique_tables_monthly,
    u.monthly.avg_queries_per_user AS avg_queries_per_user,
    u.monthly.delta_unique_users AS user_change,
    LAG(u.monthly.unique_users) OVER (ORDER BY u.date) AS prev_users,
    LAG(u.monthly.total_queries) OVER (ORDER BY u.date) AS prev_queries
  FROM product_analytics_staging.agg_table_usage_rolling u
  JOIN product_analytics_staging.dim_cost_hierarchies h
    USING (date, cost_hierarchy_id)
  WHERE h.grouping_columns = 'team'
    AND h.team = :team
    AND u.date BETWEEN :date.min AND :date.max
)
SELECT 
  CAST(date AS DATE) AS date,
  
  -- Core metrics
  monthly_active_users,
  monthly_queries,
  unique_tables_monthly,
  ROUND(avg_queries_per_user, 1) AS avg_queries_per_user,
  
  -- Growth metrics (0-1 range for UI percent formatting)
  user_change,
  ROUND(user_change * 1.0 / NULLIF(monthly_active_users - user_change, 0), 4) AS user_growth_pct,
  ROUND((monthly_queries - prev_queries) * 1.0 / NULLIF(prev_queries, 0), 4) AS query_growth_pct

FROM latest_data
ORDER BY date DESC
LIMIT 1

