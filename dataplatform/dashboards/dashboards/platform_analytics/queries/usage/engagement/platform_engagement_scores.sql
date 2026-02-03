-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: User Analytics - Platform Engagement
-- View: Engagement Scores (Team Tables)
-- =============================================================================
-- Visualization: Scatter plot + table
-- Title: "User Engagement Scores for FirmwareVDP Tables"
-- Description: Engagement scores based on usage of team-owned tables.
--              Score combines query volume, breadth (unique tables), and recency.
--
-- Scatter Plot:
--   X-Axis: volume_score (0-1)
--   Y-Axis: breadth_score (0-1)
--   Point Size: engagement_score
--   Color: engagement_tier
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

max_values AS (
  SELECT 
    MAX(total_queries) AS max_queries,
    MAX(unique_tables) AS max_tables,
    MAX(active_days) AS max_active_days
  FROM user_activity
),

user_scores AS (
  SELECT 
    u.*,
    -- Normalized scores (0-1)
    u.total_queries * 1.0 / NULLIF(m.max_queries, 0) AS volume_score,
    u.unique_tables * 1.0 / NULLIF(m.max_tables, 0) AS breadth_score,
    u.active_days * 1.0 / NULLIF(m.max_active_days, 0) AS recency_score
  FROM user_activity u
  CROSS JOIN max_values m
)

SELECT 
  employee_email,
  department,
  job_family,
  job_family_group,
  
  -- Component scores (0-1, UI formats as percentage)
  ROUND(volume_score, 3) AS volume_score,
  ROUND(breadth_score, 3) AS breadth_score,
  ROUND(recency_score, 3) AS recency_score,
  
  -- Weighted engagement score (0-1):
  -- Recency (40%) + Frequency/Volume (30%) + Breadth (30%)
  ROUND(
    recency_score * 0.40 +
    volume_score * 0.30 +
    breadth_score * 0.30,
    3
  ) AS engagement_score,
  
  -- Raw metrics
  total_queries,
  unique_tables,
  active_days,
  
  -- Engagement tier (thresholds: >0.70 HIGH, >=0.40 MEDIUM, else LOW)
  CASE 
    WHEN (recency_score * 0.40 + volume_score * 0.30 + breadth_score * 0.30) > 0.70 THEN 'HIGH_ENGAGEMENT'
    WHEN (recency_score * 0.40 + volume_score * 0.30 + breadth_score * 0.30) >= 0.40 THEN 'MEDIUM_ENGAGEMENT'
    ELSE 'LOW_ENGAGEMENT'
  END AS engagement_tier

FROM user_scores
ORDER BY engagement_score DESC
LIMIT CAST(CASE WHEN :limit <= 0 THEN 999999 ELSE :limit END AS INT)
