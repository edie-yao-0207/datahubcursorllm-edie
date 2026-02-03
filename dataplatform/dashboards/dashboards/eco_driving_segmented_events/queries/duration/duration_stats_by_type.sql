-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Duration Analysis
-- View: Duration Statistics by Event Type
-- =============================================================================
-- Visualization: Table
-- Title: "Duration Statistics by Event Type"
-- Description: Min, max, average duration statistics per event type
-- =============================================================================

-- Note: With pre-aggregated data, we can compute weighted averages and min/max
-- but not exact percentiles across the full distribution
SELECT
    event_type,
    SUM(duration_count) AS event_count,
    MIN(min_duration_ms) / 1000.0 AS min_duration_s,
    MAX(max_duration_ms) / 1000.0 AS max_duration_s,
    SUM(sum_duration_ms) / NULLIF(SUM(duration_count), 0) / 1000.0 AS avg_duration_s,
    -- Approximate median using weighted average of device-level averages
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_duration_ms) / 1000.0 AS approx_median_duration_s,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY avg_duration_ms) / 1000.0 AS approx_p95_duration_s
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
  AND duration_count > 0
GROUP BY event_type
ORDER BY event_count DESC
