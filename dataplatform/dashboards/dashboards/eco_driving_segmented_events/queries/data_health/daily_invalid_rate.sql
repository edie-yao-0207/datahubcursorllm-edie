-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Data Health
-- View: Daily Invalid Rate by Event Type
-- =============================================================================
-- Visualization: Line Chart
-- Title: "Daily Invalid Rate by Event Type"
-- Description: Daily percentage of invalid rows per event type
-- =============================================================================

SELECT
    event_type,
    date AS event_date,
    100.0 * SUM(invalid_count) / SUM(event_count) AS percent_invalid
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
GROUP BY event_type, date
ORDER BY date, event_type
