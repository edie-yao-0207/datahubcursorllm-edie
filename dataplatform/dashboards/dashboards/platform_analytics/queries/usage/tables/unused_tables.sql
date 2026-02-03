-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Usage - Tables
-- View: Unused Tables
-- =============================================================================
-- Visualization: Table
-- Title: "Unused Tables (Deprecation Candidates)"
-- Description: Tables with no queries in the selected date range.
--              Good candidates for archival or cost reduction.
-- =============================================================================

WITH team_tables AS (
  -- Get all tables owned by the team from dim_tables
  SELECT DISTINCT
    table_id,
    region,
    database,
    `table`
  FROM product_analytics_staging.dim_tables
  WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_tables WHERE date BETWEEN :date.min AND :date.max)
    AND team = :team
),

-- Aggregate usage from fact table
recent_usage AS (
  SELECT
    q.table_id,
    SUM(q.query_count) AS total_queries,
    COUNT(DISTINCT q.employee_id) AS total_users,
    MAX(q.date) AS last_query_date
  FROM product_analytics_staging.fct_table_user_queries q
  WHERE q.date BETWEEN :date.min AND :date.max
  GROUP BY q.table_id
)

-- Find tables with no usage (LEFT JOIN to find tables with no queries)
SELECT
  t.region,
  t.database,
  t.`table`,
  COALESCE(u.total_queries, 0) AS total_queries,
  COALESCE(u.total_users, 0) AS total_users,
  CAST(u.last_query_date AS DATE) AS last_query_date,
  DATEDIFF(CURRENT_DATE(), u.last_query_date) AS days_since_last_query,
  -- Quick navigation links
  CONCAT('https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,', t.database, '.', t.`table`, ',PROD)/Schema') AS datahub_url,
  CONCAT('https://dagster.internal.samsara.com/assets/', t.region, '/', t.database, '/', t.`table`) AS dagster_url
FROM team_tables t
LEFT JOIN recent_usage u
  USING (table_id)
WHERE u.total_queries IS NULL
   OR u.total_queries = 0
ORDER BY t.database, t.`table`
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
