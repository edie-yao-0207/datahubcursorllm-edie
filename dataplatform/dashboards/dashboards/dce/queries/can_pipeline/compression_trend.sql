-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Compression Ratio Trend
-- =============================================================================
-- Visualization: Line chart
-- Description: Compression ratio over time
-- =============================================================================

WITH daily_raw_frames AS (
  SELECT 
    date,
    COUNT(*) AS raw_count
  FROM product_analytics_staging.fct_sampled_can_frames
  WHERE date BETWEEN :date.min AND :date.max
    AND payload IS NOT NULL
    AND LENGTH(payload) > 0
    AND source_interface != 'j1587'
  GROUP BY date
),
daily_recompiled_frames AS (
  SELECT 
    date,
    COUNT(*) AS recompiled_count
  FROM product_analytics_staging.fct_can_trace_recompiled
  WHERE date BETWEEN :date.min AND :date.max
  GROUP BY date
)

SELECT
  drf.date,
  drf.raw_count,
  drc.recompiled_count,
  ROUND(drf.raw_count::FLOAT / NULLIF(drc.recompiled_count, 0), 2) AS compression_ratio,
  ROUND((1.0 - drc.recompiled_count::FLOAT / drf.raw_count::FLOAT), 4) AS deduplication_reduction_pct
FROM daily_raw_frames drf
JOIN daily_recompiled_frames drc ON drf.date = drc.date
ORDER BY drf.date

