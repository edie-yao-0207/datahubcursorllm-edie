-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Depth
-- View: Population Coverage
-- =============================================================================
-- Visualization: Line Chart
-- Description: Track unique populations with promotions over time with rolling windows
-- Note: Computes accurate window-level distinct counts from source fact table
-- =============================================================================

WITH date_spine AS (
  SELECT explode(sequence(CAST(:date.min AS DATE), CAST(:date.max AS DATE), interval 1 day)) AS date
),

-- Get all date/population combinations from source
-- Lookback from :date.min minus 89 days to ensure quarterly windows have full data
date_populations AS (
  SELECT DISTINCT date, population_uuid
  FROM product_analytics_staging.fct_signal_promotion_latest_state
  WHERE date BETWEEN DATE_SUB(CAST(:date.min AS DATE), 89) AND CAST(:date.max AS DATE)
),

-- Compute rolling window distinct counts via self-join
rolling_counts AS (
  SELECT
    ds.date,
    COUNT(DISTINCT CASE WHEN src.date = ds.date THEN src.population_uuid END) AS daily_unique_populations,
    COUNT(DISTINCT CASE WHEN src.date BETWEEN DATE_SUB(ds.date, 6) AND ds.date THEN src.population_uuid END) AS weekly_unique_populations,
    COUNT(DISTINCT CASE WHEN src.date BETWEEN DATE_SUB(ds.date, 29) AND ds.date THEN src.population_uuid END) AS monthly_unique_populations,
    COUNT(DISTINCT CASE WHEN src.date BETWEEN DATE_SUB(ds.date, 89) AND ds.date THEN src.population_uuid END) AS quarterly_unique_populations
  FROM date_spine ds
  LEFT JOIN date_populations src ON src.date BETWEEN DATE_SUB(ds.date, 89) AND ds.date
  WHERE ds.date BETWEEN CAST(:date.min AS DATE) AND CAST(:date.max AS DATE)
  GROUP BY ds.date
)

SELECT
  date,
  daily_unique_populations,
  weekly_unique_populations,
  monthly_unique_populations,
  quarterly_unique_populations
FROM rolling_counts
ORDER BY date DESC

