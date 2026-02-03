-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Overview
-- View: Normalized Events per Device by Type
-- =============================================================================
-- Visualization: Table
-- Title: "Normalized Events per Device by Type"
-- Description: Shows average, P95, and P99 event counts per device for each event type
-- =============================================================================

WITH device_totals AS (
    -- Sum up each device's events across the date range
    SELECT 
        event_type, 
        object_id, 
        SUM(event_count) AS event_count
    FROM product_analytics_staging.agg_eco_driving_segmented_events
    WHERE date BETWEEN :date.min AND :date.max
    GROUP BY event_type, object_id
)

SELECT
    event_type,
    COUNT(DISTINCT object_id) AS device_count,
    SUM(event_count) AS total_events,
    ROUND(AVG(event_count), 2) AS avg_events_per_device,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY event_count), 2) AS median_events_per_device,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY event_count), 2) AS p95_events_per_device,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY event_count), 2) AS p99_events_per_device,
    MIN(event_count) AS min_events_per_device,
    MAX(event_count) AS max_events_per_device
FROM device_totals
GROUP BY event_type
ORDER BY total_events DESC
