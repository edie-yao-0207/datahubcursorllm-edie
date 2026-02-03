-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: User Analytics - Platform Engagement
-- View: Power Users of Team Tables
-- =============================================================================
-- Visualization: Table with tier badges
-- Title: "Power Users of FirmwareVDP Tables"
-- Description: Users ranked by their usage of tables owned by the selected team.
--              Pure star schema: joins fact to dim_tables and employee_hierarchy.
--
-- Annotations:
--   - SUPER_USER: Top 5% by total_queries
--   - POWER_USER: Top 10% (P90-P95)
--   - ACTIVE_USER: Top 25% (P75-P90)
--   - REGULAR: Bottom 75%
-- =============================================================================

-- Subtract 1 day from max to ensure complete data
WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.fct_table_user_queries
  WHERE date BETWEEN :date.min AND DATE_SUB(CAST(:date.max AS DATE), 1)
),

-- Aggregate user activity on team-owned tables (last 30 days)
-- Join to both dimensions for filtering and attributes
user_activity AS (
  SELECT 
    e.employee_email,
    e.job_family_group,
    e.job_family,
    e.department,
    SUM(q.query_count) AS total_queries,
    COUNT(DISTINCT q.table_id) AS unique_tables,
    COUNT(DISTINCT q.date) AS active_days
  FROM product_analytics_staging.fct_table_user_queries q
  JOIN product_analytics_staging.dim_tables t USING (date, table_id)
  JOIN edw.silver.employee_hierarchy_active_vw e ON q.employee_id = e.employee_id
  WHERE q.date BETWEEN DATE_SUB((SELECT max_date FROM latest_date), 29) AND (SELECT max_date FROM latest_date)
    AND t.team = :team
  GROUP BY e.employee_email, e.job_family_group, e.job_family, e.department
  HAVING SUM(q.query_count) > 0
),

percentiles AS (
  SELECT 
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_queries) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_queries) AS p90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_queries) AS p95
  FROM user_activity
)

SELECT 
  ROW_NUMBER() OVER (ORDER BY u.total_queries DESC) AS rank,
  u.employee_email,
  u.department,
  u.job_family,
  u.job_family_group,
  u.total_queries,
  u.unique_tables,
  u.active_days,
  ROUND(u.total_queries * 1.0 / NULLIF(u.active_days, 0), 1) AS queries_per_day,
  CASE 
    WHEN u.total_queries >= p.p95 THEN 'SUPER_USER'
    WHEN u.total_queries >= p.p90 THEN 'POWER_USER'
    WHEN u.total_queries >= p.p75 THEN 'ACTIVE_USER'
    ELSE 'REGULAR'
  END AS user_tier,
  ROUND(PERCENT_RANK() OVER (ORDER BY u.total_queries) * 100, 1) AS percentile_rank
FROM user_activity u
CROSS JOIN percentiles p
ORDER BY u.total_queries DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
