-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Duration Analysis
-- View: Duration Distribution - Overspeed
-- =============================================================================
-- Visualization: Bar Chart (Histogram)
-- Title: "Duration Distribution - Overspeed"
-- Description: Distribution of average durations per device for Overspeed events
-- =============================================================================

SELECT
    FLOOR((avg_duration_ms / 1000.0) / 0.5) * 0.5 AS duration_bucket_s,
    COUNT(*) AS device_count
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
  AND event_type = 'OverSpeed'
  AND avg_duration_ms IS NOT NULL
GROUP BY duration_bucket_s
ORDER BY duration_bucket_s
LIMIT 100
