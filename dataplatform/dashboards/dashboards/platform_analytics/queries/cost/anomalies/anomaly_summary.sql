-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Anomalies
-- View: Anomaly Summary
-- =============================================================================
-- Visualization: 3 Stacked Bar Charts (or 1 chart with time_horizon filter)
-- Title: "Anomaly Count by Severity & Time Horizon"
-- Description: Count of warnings, anomalous, and critical cost deviations.
--              Unpivoted for easy charting - filter by time_horizon for separate views.
--
-- X-Axis: date
-- Y-Axis: count (stacked)
-- Series (3 only): warnings, anomalous, critical
--
-- Annotations:
--   - Horizontal line at 5 on Y-axis (label: "Alert Threshold", color: yellow)
--     (More than 5 anomalies/day warrants investigation)
--   - Horizontal line at 10 on Y-axis (label: "Critical Threshold", color: red)
--     (More than 10 anomalies/day requires immediate action)
-- =============================================================================

WITH base_data AS (
  SELECT 
    a.date,
    h.team,
    h.grouping_columns,
    a.daily,
    a.weekly,
    a.monthly
  FROM product_analytics_staging.fct_table_cost_anomalies a
  JOIN product_analytics_staging.dim_cost_hierarchies h
    USING (date, cost_hierarchy_id)
  WHERE a.date BETWEEN :date.min AND :date.max
    AND h.grouping_columns = 'region.team.product_group.dataplatform_feature.service'
    AND h.team = :team
),
unpivoted AS (
  SELECT 
    date,
    team,
    grouping_columns,
    'daily' AS time_horizon,
    CASE WHEN daily.status = 'warning' THEN 1 ELSE 0 END AS warnings,
    CASE WHEN daily.status IN ('anomalous_increase', 'anomalous_decrease') THEN 1 ELSE 0 END AS anomalous,
    CASE WHEN daily.status IN ('critical_increase', 'critical_decrease') THEN 1 ELSE 0 END AS critical,
    daily.cost AS cost
  FROM base_data
  
  UNION ALL
  
  SELECT 
    date,
    team,
    grouping_columns,
    'weekly' AS time_horizon,
    CASE WHEN weekly.status = 'warning' THEN 1 ELSE 0 END AS warnings,
    CASE WHEN weekly.status IN ('anomalous_increase', 'anomalous_decrease') THEN 1 ELSE 0 END AS anomalous,
    CASE WHEN weekly.status IN ('critical_increase', 'critical_decrease') THEN 1 ELSE 0 END AS critical,
    weekly.cost AS cost
  FROM base_data
  
  UNION ALL
  
  SELECT 
    date,
    team,
    grouping_columns,
    'monthly' AS time_horizon,
    CASE WHEN monthly.status = 'warning' THEN 1 ELSE 0 END AS warnings,
    CASE WHEN monthly.status IN ('anomalous_increase', 'anomalous_decrease') THEN 1 ELSE 0 END AS anomalous,
    CASE WHEN monthly.status IN ('critical_increase', 'critical_decrease') THEN 1 ELSE 0 END AS critical,
    monthly.cost AS cost
  FROM base_data
)
SELECT 
  CAST(date AS DATE) AS date,
  time_horizon,
  SUM(warnings) AS warnings,
  SUM(anomalous) AS anomalous,
  SUM(critical) AS critical,
  SUM(warnings) + SUM(anomalous) + SUM(critical) AS total_anomalies
FROM unpivoted
WHERE time_horizon = 'daily'  -- Filter to daily for the dashboard chart
GROUP BY date, time_horizon
ORDER BY date DESC
