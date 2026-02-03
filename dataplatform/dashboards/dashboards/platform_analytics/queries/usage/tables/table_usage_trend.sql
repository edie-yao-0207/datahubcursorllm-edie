-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage - Tables
-- View: Table Usage Trend
-- =============================================================================
-- Visualization: Line Chart
-- Title: "Table Usage Trends (7-Day Rolling)"
-- Description: Daily query and user counts with 7-day rolling averages.
--              Declining trends may indicate deprecation candidates.
-- X-Axis: date
-- Y-Axis: queries_7d_avg
-- =============================================================================

-- Aggregate from fact table, join to dim for attributes
-- Subtract 1 day from max to ensure complete data
WITH daily_usage AS (
  SELECT
    q.date,
    t.region,
    t.database,
    t.`table`,
    SUM(q.query_count) AS num_queries,
    COUNT(DISTINCT q.employee_id) AS num_users
  FROM product_analytics_staging.fct_table_user_queries q
  JOIN product_analytics_staging.dim_tables t USING (date, table_id)
  WHERE q.date BETWEEN :date.min AND DATE_SUB(CAST(:date.max AS DATE), 1)
    AND t.team = :team
  GROUP BY q.date, t.region, t.database, t.`table`
)

SELECT
  CAST(date AS DATE) AS date,
  region,
  database,
  `table`,
  num_queries,
  num_users,
  -- 7-day rolling averages
  ROUND(AVG(num_queries) OVER (
    PARTITION BY region, database, `table` 
    ORDER BY date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 1) AS queries_7d_avg,
  ROUND(AVG(num_users) OVER (
    PARTITION BY region, database, `table` 
    ORDER BY date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 1) AS users_7d_avg,
  -- Quick navigation links
  CONCAT('https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,', database, '.', `table`, ',PROD)/Schema') AS datahub_url,
  CONCAT('https://dagster.internal.samsara.com/assets/', region, '/', database, '/', `table`) AS dagster_url
FROM daily_usage
ORDER BY region, database, `table`, date
