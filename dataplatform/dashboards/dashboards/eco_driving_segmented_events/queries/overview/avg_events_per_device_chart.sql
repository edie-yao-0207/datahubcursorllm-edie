-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Overview
-- View: Average Events per Device by Type Over Time
-- =============================================================================
-- Visualization: Line Chart
-- Title: "Normalized Events per Device Over Time"
-- Description: Daily average, P95, and P99 event counts per device for each event type
-- =============================================================================

-- Note: Using pre-aggregated data, we compute per-device stats from daily aggregates
SELECT
    date AS event_date,
    event_type,
    ROUND(AVG(event_count), 2) AS avg_events_per_device,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY event_count), 2) AS p95_events_per_device,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY event_count), 2) AS p99_events_per_device
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
GROUP BY date, event_type
ORDER BY date, event_type
