-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Comparison
-- View: Full Event Type Comparison
-- =============================================================================
-- Visualization: Table
-- Title: "Full Event Type Comparison"
-- Description: Comprehensive comparison of all event types including volume, data quality, and duration metrics
-- =============================================================================

WITH stats AS (
    SELECT
        event_type,
        SUM(event_count) AS total_rows,
        SUM(valid_count) AS valid_rows,
        SUM(invalid_count) AS invalid_rows,
        COUNT(DISTINCT object_id) AS unique_devices,
        SUM(sum_duration_ms) / NULLIF(SUM(duration_count), 0) / 1000.0 AS avg_duration_s,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_duration_ms) / 1000.0 AS approx_median_duration_s
    FROM product_analytics_staging.agg_eco_driving_segmented_events
    WHERE date BETWEEN :date.min AND :date.max
    GROUP BY event_type
),
totals AS (
    SELECT SUM(total_rows) AS grand_total FROM stats
)

SELECT
    s.event_type,
    s.total_rows,
    s.valid_rows,
    s.invalid_rows,
    100.0 * s.valid_rows / NULLIF(s.total_rows, 0) AS percent_valid,
    s.unique_devices,
    s.avg_duration_s,
    s.approx_median_duration_s AS median_duration_s,
    100.0 * s.total_rows / t.grand_total AS percent_of_total
FROM stats s
CROSS JOIN totals t
ORDER BY s.total_rows DESC
