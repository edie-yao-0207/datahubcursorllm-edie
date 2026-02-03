-- =============================================================================
-- Dashboard: Signal Promotion Metrics
-- Tab: Depth
-- View: Signal Diversity
-- =============================================================================
-- Visualization: Line Chart
-- Description: Track unique signals promoted over time with rolling windows
-- Note: Computes accurate window-level distinct counts from source fact table
-- =============================================================================

WITH date_spine AS (
  SELECT explode(sequence(CAST(:date.min AS DATE), CAST(:date.max AS DATE), interval 1 day)) AS date
),

-- Get all date/signal combinations from source
-- Lookback from :date.min minus 89 days to ensure quarterly windows have full data
date_signals AS (
  SELECT DISTINCT date, signal_uuid
  FROM product_analytics_staging.fct_signal_promotion_latest_state
  WHERE date BETWEEN DATE_SUB(CAST(:date.min AS DATE), 89) AND CAST(:date.max AS DATE)
),

-- Compute rolling window distinct counts via self-join
rolling_counts AS (
  SELECT
    ds.date,
    COUNT(DISTINCT CASE WHEN src.date = ds.date THEN src.signal_uuid END) AS daily_unique_signals,
    COUNT(DISTINCT CASE WHEN src.date BETWEEN DATE_SUB(ds.date, 6) AND ds.date THEN src.signal_uuid END) AS weekly_unique_signals,
    COUNT(DISTINCT CASE WHEN src.date BETWEEN DATE_SUB(ds.date, 29) AND ds.date THEN src.signal_uuid END) AS monthly_unique_signals,
    COUNT(DISTINCT CASE WHEN src.date BETWEEN DATE_SUB(ds.date, 89) AND ds.date THEN src.signal_uuid END) AS quarterly_unique_signals
  FROM date_spine ds
  LEFT JOIN date_signals src ON src.date BETWEEN DATE_SUB(ds.date, 89) AND ds.date
  WHERE ds.date BETWEEN CAST(:date.min AS DATE) AND CAST(:date.max AS DATE)
  GROUP BY ds.date
)

SELECT
  date,
  daily_unique_signals,
  weekly_unique_signals,
  monthly_unique_signals,
  quarterly_unique_signals
FROM rolling_counts
ORDER BY date DESC

