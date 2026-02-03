-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Candidates
-- View: Top Candidates by Rank
-- =============================================================================
-- Visualization: Table
-- Title: Top Candidates by Rank
-- Description: Highest priority candidates across all query types
-- =============================================================================

WITH latest_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.fct_can_traces_required
    WHERE date BETWEEN :date.min AND :date.max
),

all_candidates AS (
    SELECT 
        date,
        org_id,
        device_id,
        start_time,
        'can-set-main-0' AS query_type,
        global_trace_rank AS rank
    FROM datamodel_dev.fct_can_trace_representative_candidates
    WHERE date = (SELECT max_date FROM latest_date)
    
    UNION ALL
    
    SELECT 
        date,
        org_id,
        device_id,
        start_time,
        'can-set-training-0' AS query_type,
        global_trace_rank AS rank
    FROM datamodel_dev.fct_can_trace_reverse_engineering_candidates
    WHERE date = (SELECT max_date FROM latest_date)
      AND for_training = TRUE
    
    UNION ALL
    
    SELECT 
        date,
        org_id,
        device_id,
        start_time,
        'can-set-inference-0' AS query_type,
        global_trace_rank AS rank
    FROM datamodel_dev.fct_can_trace_reverse_engineering_candidates
    WHERE date = (SELECT max_date FROM latest_date)
      AND for_inference = TRUE
    
    UNION ALL
    
    SELECT 
        date,
        org_id,
        device_id,
        start_time,
        'can-set-training-stream-id-0' AS query_type,
        global_trace_rank AS rank
    FROM datamodel_dev.fct_can_trace_training_by_stream_id
    WHERE date = (SELECT max_date FROM latest_date)
    
    UNION ALL
    
    SELECT 
        date,
        org_id,
        device_id,
        start_time,
        'can-set-seatbelt-trip-start-0' AS query_type,
        global_trace_rank AS rank
    FROM datamodel_dev.fct_can_trace_seatbelt_trip_start_candidates
    WHERE date = (SELECT max_date FROM latest_date)
),

-- Join with vehicle properties and definition tables for translations
candidates_with_properties AS (
    SELECT
        ac.query_type,
        ac.org_id,
        ac.device_id,
        ac.start_time,
        ac.rank,
        dvp.make,
        dvp.model,
        dvp.year,
        dvp.engine_model,
        dvp.powertrain AS powertrain_id,
        dvp.fuel_group AS fuel_group_id,
        dvp.trim,
        pt.name AS powertrain,
        fg.name AS fuel_group
    FROM all_candidates ac
    LEFT JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON ac.date = dvp.date
        AND ac.org_id = dvp.org_id
        AND ac.device_id = dvp.device_id
    LEFT JOIN definitions.properties_fuel_powertrain pt ON pt.id = dvp.powertrain
    LEFT JOIN definitions.properties_fuel_fuelgroup fg ON fg.id = dvp.fuel_group
)

SELECT
    query_type,
    org_id,
    device_id,
    start_time,
    rank,
    make,
    model,
    year,
    engine_model,
    powertrain,
    fuel_group,
    trim
FROM candidates_with_properties
ORDER BY rank ASC
LIMIT 100
