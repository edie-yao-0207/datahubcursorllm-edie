-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Overview
-- View: Malfunction Summary
-- =============================================================================
-- Visualization: Counter
-- Title: "Total Malfunctions (30-Day)"
-- Description: Total count of all malfunction events in the selected date range
-- =============================================================================

WITH
-- Explode ELD events to get individual records
eld_events_exploded AS (
    SELECT
        date,
        org_id,
        object_id AS device_id,
        time,
        eld_event.type AS event_type,
        record.event_type AS record_event_type,
        record.diagnostic_or_malfunction.event_code AS malfunction_event_code,
        record.diagnostic_or_malfunction.diagnostic_or_malfunction_code AS diagnostic_or_malfunction_code,
        record.event_at_utc_ms
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
        ese.*,
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
    COUNT(*) AS total_malfunctions
FROM malfunctions_with_dimensions

