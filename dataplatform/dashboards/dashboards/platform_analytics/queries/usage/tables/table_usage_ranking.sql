-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage - Tables
-- View: Table Usage Ranking
-- =============================================================================
-- Visualization: Table
-- Title: "Top Tables by Query Volume"
-- Description: Tables ranked by query count. High query tables may need cost
--              monitoring. Low users with high queries may indicate automation.
-- =============================================================================

-- Subtract 1 day from max to ensure complete data
WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.fct_table_user_queries
  WHERE date BETWEEN :date.min AND DATE_SUB(CAST(:date.max AS DATE), 1)
),

-- Aggregate from fact table, join to dim for attributes
table_usage AS (
  SELECT
    t.region,
    t.database,
    t.`table`,
    SUM(q.query_count) AS num_queries,
    COUNT(DISTINCT q.employee_id) AS num_users
  FROM product_analytics_staging.fct_table_user_queries q
  JOIN product_analytics_staging.dim_tables t USING (date, table_id)
  WHERE q.date = (SELECT max_date FROM latest_date)
    AND t.team = :team
  GROUP BY t.region, t.database, t.`table`
)

SELECT
  region,
  database,
  `table`,
  num_queries,
  num_users,
  ROUND(num_queries / NULLIF(num_users, 0), 1) AS queries_per_user,
  -- Quick navigation links
  CONCAT('https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,', database, '.', `table`, ',PROD)/Schema') AS datahub_url,
  CONCAT('https://dagster.internal.samsara.com/assets/', region, '/', database, '/', `table`) AS dagster_url
FROM table_usage
WHERE num_queries > 0
ORDER BY num_queries DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
