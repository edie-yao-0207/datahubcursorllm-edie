-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Trends
-- View: Malfunction Trend by Type
-- =============================================================================
-- Visualization: Line
-- Title: "Malfunction Trend by Type"
-- Description: Daily trend of malfunctions broken down by type
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
),

-- Translate codes to human-readable names
malfunctions_with_labels AS (
    SELECT
        mwd.date,
        mwd.org_id,
        mwd.device_id,
        mwd.make,
        mwd.model,
        mwd.year,
        mwd.cable_name,
        mwd.product_name,
        CASE mwd.diagnostic_or_malfunction_code
            WHEN 'P' THEN 'Power Malfunction'
            WHEN 'E' THEN 'Engine Sync Malfunction'
            WHEN 'T' THEN 'Timing Malfunction'
            WHEN 'L' THEN 'Positioning Malfunction'
            WHEN 'R' THEN 'Data Recording Malfunction'
            WHEN 'S' THEN 'Data Transfer Malfunction'
            WHEN 'O' THEN 'Other ELD Malfunction'
            WHEN '1' THEN 'Power Diagnostic'
            WHEN '2' THEN 'Engine Sync Diagnostic'
            WHEN '3' THEN 'Missing Required Data Diagnostic'
            WHEN '4' THEN 'Data Transfer Diagnostic'
            WHEN '5' THEN 'Unidentified Driving Diagnostic'
            WHEN '6' THEN 'Other ELD Diagnostic'
            ELSE CONCAT('Unknown (', mwd.diagnostic_or_malfunction_code, ')')
        END AS malfunction_type_label
    FROM malfunctions_with_dimensions mwd
)

SELECT
    date,
    malfunction_type_label AS malfunction_type,
    COUNT(*) AS daily_malfunction_count
FROM malfunctions_with_labels
GROUP BY date, malfunction_type_label
ORDER BY date, malfunction_type_label

