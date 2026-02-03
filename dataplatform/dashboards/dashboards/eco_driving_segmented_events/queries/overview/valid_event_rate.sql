-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Overview
-- View: Valid Event Rate
-- =============================================================================
-- Visualization: Counter
-- Title: "Valid Event Rate"
-- Description: Percentage of events with valid proto_value
-- =============================================================================

SELECT
    100.0 * SUM(valid_count) / SUM(event_count) AS valid_rate_pct
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
