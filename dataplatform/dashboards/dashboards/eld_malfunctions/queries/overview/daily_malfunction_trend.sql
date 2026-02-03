-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Overview
-- View: Daily Malfunction Trend
-- =============================================================================
-- Visualization: Line
-- Title: "Daily Malfunction Trend"
-- Description: Daily count of malfunctions over time
-- =============================================================================

WITH
-- Explode ELD events to get individual records
eld_events_exploded AS (
    SELECT
        CAST(date AS DATE) AS date,
        org_id,
        object_id AS device_id,
        record.diagnostic_or_malfunction.diagnostic_or_malfunction_code AS diagnostic_or_malfunction_code
    FROM kinesisstats_history.osDEldEvent
    LATERAL VIEW explode(value.proto_value.eld_events) exploded AS eld_event
    LATERAL VIEW explode(eld_event.records) exploded_records AS record
    WHERE date BETWEEN :date.min AND :date.max
      AND record.event_type = 7  -- ET_DIAGNOSTIC_OR_MALFUNCTION
      AND record.diagnostic_or_malfunction.diagnostic_or_malfunction_code IS NOT NULL
      AND record.diagnostic_or_malfunction.event_code = 1  -- EC_MALFUNCTION_CREATED
),

-- Join with vehicle properties and device dimensions for filtering
malfunctions_with_dimensions AS (
    SELECT
        ese.date,
        ese.org_id,
        ese.device_id,
        ese.diagnostic_or_malfunction_code,
        dvp.make,
        dvp.model,
        dvp.year,
        ddd.cable_name,
        ddd.product_name
    FROM eld_events_exploded ese
    LEFT JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON ese.org_id = dvp.org_id
        AND ese.device_id = dvp.device_id
        AND dvp.date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
    LEFT JOIN product_analytics.dim_device_dimensions ddd
        ON ese.org_id = ddd.org_id
        AND ese.device_id = ddd.device_id
        AND ddd.date = (SELECT MAX(date) FROM product_analytics.dim_device_dimensions)
)

SELECT
    date,
    COUNT(*) AS daily_malfunction_count
FROM malfunctions_with_dimensions
GROUP BY date
ORDER BY date

