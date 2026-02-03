-- =============================================================================
-- Dashboard: Data Cost & Efficiency (DCE)
-- Tab: Cost Attribution
-- View: Service Cost History
-- =============================================================================
-- Visualization: Line Chart (multi-series) + Table
-- Title: "Service Cost Over Time"
-- Description: Drill into individual service costs over time. Shows daily, weekly,
--              and monthly cost trends. Use for investigating cost changes.
--
-- Parameters:
--   :service (Text) - Filter to specific service (optional, default shows all)
--   :team (Text) - Filter by team owner (default: 'firmwarevdp')
--
-- X-Axis: date
-- Y-Axis: cost (USD)
-- Series: daily_cost, rolling_7d_cost, rolling_30d_cost
--
-- Annotations:
--   - Horizontal line at $0 (label: "Zero Cost", style: dashed)
--   - Per-service: Previous month's average as baseline (dynamic annotation)
--   - Optional: Vertical lines on known release/config change dates
-- =============================================================================

SELECT 
  CAST(c.date AS DATE) AS date,
  h.region,
  h.team,
  h.service,
  h.product_group,
  h.dataplatform_feature,
  c.daily.cost AS daily_cost,
  c.weekly.cost AS rolling_7d_cost,
  c.monthly.cost AS rolling_30d_cost,
  c.daily.delta AS daily_change,
  c.monthly.delta AS monthly_change,
  c.monthly.cost / SUM(c.monthly.cost) OVER (PARTITION BY c.date) AS percent_of_team_total,
  -- Quick navigation links (service = database.table format)
  CONCAT('https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,', h.service, ',PROD)/Schema') AS datahub_url,
  CONCAT('https://dagster.internal.samsara.com/assets/', h.region, '/', REPLACE(h.service, '.', '/')) AS dagster_url
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.service IS NOT NULL  -- Service-level groupings only
  AND h.team = :team
  AND c.date BETWEEN :date.min AND :date.max
ORDER BY c.date DESC, c.monthly.cost DESC
