"""
fct_reverse_engineering_trace_candidates

Identifies CAN trace recording windows with ML training and inference candidate classification
per signal type. Each trace is labeled for:

**Training Candidates**: Devices with telematics coverage for specific signal types
- Training Candidate: Any device that has coverage (good for ML training)  
- Preferred Training Candidate: Device with coverage AND broadcast + non-standard signals
  (ideal for ML training with rich non-standard data)

**Inference Candidates**: Devices without coverage but with non-standard traffic to analyze
- Inference Candidate: Device lacks coverage for signal types but has actual non-standard 
  broadcast traffic available (good for applying trained models to reverse engineer signals)

Enhanced with signal decoding rule mappings that link coverage types to OBD promotions,
broadcast/standard signal classification, and device-level non-standard traffic detection
for targeted ML model training and inference based on signal improvement needs.
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    map_of,
    struct_with_comments,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataclasses import replace
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

from .fct_can_recording_windows import START_TIME_COLUMN, END_TIME_COLUMN

QUERY = """
WITH 
-- Translation mapping: coverage type to OBD value (expandable)
type_to_obd_mapping AS (
    SELECT * FROM VALUES
        ('odometer', CAST(1 AS BIGINT)),
        ('fuel_level_percent', CAST(5 AS BIGINT)),
        ('engine_seconds', CAST(157 AS BIGINT))
    AS mapping(type, obd_value)
),

eligible_windows AS (
    -- Get eligible recording windows: on-trip traces with 5-10 minute duration (300-600 seconds)
    SELECT 
        date,
        org_id,
        device_id,
        start_time,
        end_time
    FROM {product_analytics_staging}.fct_eligible_can_recording_windows
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
      AND on_trip
      -- 5 to 30 minutes
      AND capture_duration BETWEEN 300000 AND 1800000
),

-- Get device vehicle properties for MMYEF-based decoding rule lookups
device_vehicle_info AS (
    SELECT 
        eligible.date,
        eligible.org_id,
        eligible.device_id,
        eligible.start_time,
        eligible.end_time,
        dvp.mmyef_id
    FROM eligible_windows eligible
    LEFT JOIN {product_analytics_staging}.dim_device_vehicle_properties dvp USING (date, org_id, device_id)
),

-- Get decoding rules with broadcast/proprietary flags for enhanced candidate classification
device_decoding_rules AS (
    SELECT 
        dvi.date,
        dvi.org_id, 
        dvi.device_id,
        csd.obd_value,
        csd.is_broadcast,
        csd.is_standard,
        COUNT(*) as rule_count,
        -- Enhanced struct with broadcast/standard flags
        STRUCT(
            csd.signal_catalog_id,
            csd.obd_value,
            csd.application_id,
            csd.bit_start,
            csd.bit_length,
            csd.is_broadcast,
            csd.is_standard
        ) AS rule_summary
    FROM device_vehicle_info dvi
    JOIN {product_analytics_staging}.dim_combined_signal_definitions csd
      ON (csd.mmyef_id IS NULL OR dvi.mmyef_id = csd.mmyef_id)
    WHERE csd.obd_value IS NOT NULL
      AND csd.application_id IS NOT NULL
    GROUP BY ALL
),

-- Get all possible signal types from our mapping
all_signal_types AS (
    SELECT DISTINCT type
    FROM type_to_obd_mapping
),

-- Create complete coverage matrix: every device × every signal type
-- This ensures devices with no coverage data get coverage=false for all types
base_coverage_data AS (
    SELECT
        eligible.date,
        eligible.org_id,
        eligible.device_id,
        eligible.start_time,
        eligible.end_time,
        types.type,
        COALESCE(coverage.month_covered, FALSE) AS month_covered
      FROM eligible_windows eligible
      CROSS JOIN all_signal_types types
      LEFT JOIN product_analytics_staging.fct_telematics_coverage_rollup_full AS coverage
          USING (date, org_id, device_id, type)
),

-- Device-level proprietary traffic detection
device_proprietary_traffic AS (
    SELECT
        date, 
        org_id, 
        device_id,
        TRUE AS has_proprietary_traffic
    FROM {product_analytics_staging}.fct_diagnostic_messages_consolidated
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
    AND (
        -- NONE protocol
        protocol_summary[0].total_count > 0 
        -- J1939 proprietary PGNs
        OR SIZE(protocol_summary[2].protocol_proprietary_arbitration_ids) > 0
    )
),

-- Enhanced coverage data with broadcast/proprietary information for refined candidate classification
enhanced_coverage_data AS (
    SELECT
        base.date,
        base.org_id,
        base.device_id,
        base.start_time,
        base.end_time,
        base.type,
        base.month_covered AS is_covered,
        mapping.obd_value,
        -- Rule availability counts
        COUNT(rules.rule_count) as available_rule_count,
        SUM(COALESCE(rules.rule_count, 0)) as total_rules,
        -- Signal-specific broadcast/proprietary flags (based on this obd_value's decoding rules)
        BOOL_OR(rules.is_broadcast) AS has_broadcast_rules,
        BOOL_OR(NOT rules.is_standard) AS has_proprietary_rules,
        BOOL_OR(rules.is_broadcast AND NOT rules.is_standard) AS has_broadcast_proprietary_rules,
        -- Device-level proprietary traffic flag (applies to all signals on the device)
        FIRST(prop.has_proprietary_traffic) AS device_has_proprietary_traffic,
        -- Collect rule summaries with enhanced flags
        COLLECT_SET(rules.rule_summary) AS rule_summaries
    FROM base_coverage_data base
    LEFT JOIN type_to_obd_mapping mapping USING (type)
    LEFT JOIN device_decoding_rules rules USING (date, org_id, device_id, obd_value)
    LEFT JOIN device_proprietary_traffic prop USING (date, org_id, device_id)
    GROUP BY ALL
),

-- Add simple deduplication to prevent DUPLICATED_MAP_KEY errors
-- RAM 1500 2019 have duplicate promotions for the same signal
-- because there are different request and response IDs available.
deduplicated_coverage AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date, org_id, device_id, type 
            ORDER BY has_broadcast_proprietary_rules DESC, available_rule_count DESC, is_covered DESC, total_rules DESC
        ) AS rn
    FROM enhanced_coverage_data
),

-- Aggregate deduplicated coverage per trace segment (not per device)
coverage_per_trace AS (
    SELECT
        date,
        org_id,
        device_id,
        -- Recording window timing details (preserved per trace segment)
        start_time,
        end_time,
        
        -- Simplified signal coverage map (cleaner structure for debugging)
        MAP_FROM_ENTRIES(
            COLLECT_LIST(
                STRUCT(
                    type AS key,
                    STRUCT(
                        is_covered AS coverage,
                        obd_value,
                        available_rule_count,
                        total_rules,
                        has_broadcast_rules,
                        has_proprietary_rules,
                        has_broadcast_proprietary_rules,

                        -- Any device that has coverage
                        COALESCE(is_covered, FALSE) AS is_training_candidate,
                        -- Signal-specific: Coverage + THIS obd_value is broadcast + proprietary 
                        COALESCE(is_covered AND has_broadcast_proprietary_rules, FALSE) AS is_preferred_training_candidate,
                        -- Device-level: No coverage but device has any proprietary traffic to analyze
                        COALESCE(NOT is_covered AND device_has_proprietary_traffic, FALSE) AS is_inference_candidate
                    ) AS value
                )
            )
        ) AS signal_coverage_map,
        
        -- Summary stats for easy filtering
        COUNT(CASE WHEN is_covered = false THEN 1 END) AS coverage_gap_count,
        COUNT(CASE WHEN is_covered = true THEN 1 END) AS covered_signal_count,
        COUNT(type) AS total_signals_evaluated,
        COUNT(CASE WHEN available_rule_count > 0 THEN 1 END) AS signals_with_decoding_rules,
        
        -- Any coverage
        COUNT(CASE WHEN is_covered = true THEN 1 END) AS training_candidate_count,
        -- Signal-specific: Coverage + THIS signal's obd_value is broadcast + proprietary
        COUNT(CASE WHEN is_covered = true AND has_broadcast_proprietary_rules = true THEN 1 END) AS preferred_training_candidate_count,
        -- No coverage + device has proprietary traffic
        COUNT_IF(NOT is_covered AND COALESCE(device_has_proprietary_traffic, FALSE)) AS inference_candidate_count,
        COUNT_IF(available_rule_count = 0) AS no_promotion_count
        
    FROM deduplicated_coverage
    WHERE rn = 1  -- Take only the best entry per type (most rules, then covered)
    GROUP BY ALL
)


SELECT
    traces.date,
    traces.org_id,
    traces.device_id,
    
    -- Recording window timing details
    traces.start_time,
    traces.end_time,
    
    -- Simplified signal coverage mapping (coverage + decoding rule counts)  
    traces.signal_coverage_map,
    
    -- Coverage summary stats for easy filtering
    traces.coverage_gap_count,
    traces.covered_signal_count,  
    traces.total_signals_evaluated,
    traces.signals_with_decoding_rules,
    
    -- Candidate type summary stats
    traces.training_candidate_count,
    traces.preferred_training_candidate_count,
    traces.inference_candidate_count,
    traces.no_promotion_count,
    
    -- Training candidate: any device that has coverage (no proprietary traffic requirement)
    traces.training_candidate_count > 0 AS is_training_candidate,
    
    -- Preferred training candidate: device has signals with coverage AND those specific signals are broadcast + proprietary
    traces.preferred_training_candidate_count > 0 AS is_preferred_training_candidate,
    
    -- Inference candidate: no coverage but has proprietary broadcast traffic available to examine  
    traces.inference_candidate_count > 0 AS is_inference_candidate

FROM coverage_per_trace traces
"""

COLUMNS = [
    # Core trace identification (one row per trace segment)
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    # Recording window timing details for this specific trace segment
    replace(START_TIME_COLUMN, primary_key=False),
    replace(END_TIME_COLUMN, primary_key=False),
    # Simplified signal coverage mapping (easier to debug)
    Column(
        name="signal_coverage_map",
        type=map_of(
            DataType.STRING,
            struct_with_comments(
                ("coverage", DataType.BOOLEAN, "Whether this signal type is covered"),
                ("obd_value", DataType.LONG, "OBD value that maps to this signal type"),
                (
                    "available_rule_count",
                    DataType.LONG,
                    "Number of available decoding rules for this signal type",
                ),
                (
                    "total_rules",
                    DataType.LONG,
                    "Total decoding rules across all sources",
                ),
                (
                    "has_broadcast_rules",
                    DataType.BOOLEAN,
                    "True if has broadcast-based decoding rules available",
                ),
                (
                    "has_proprietary_rules",
                    DataType.BOOLEAN,
                    "True if has non-standard/proprietary protocol decoding rules available",
                ),
                (
                    "has_broadcast_proprietary_rules",
                    DataType.BOOLEAN,
                    "True if has broadcast AND non-standard/proprietary decoding rules (ideal for reverse engineering)",
                ),
                (
                    "is_training_candidate",
                    DataType.BOOLEAN,
                    "True if can be used for training (has coverage)",
                ),
                (
                    "is_preferred_training_candidate",
                    DataType.BOOLEAN,
                    "True if preferred for training (has coverage + broadcast + proprietary rules)",
                ),
                (
                    "is_inference_candidate",
                    DataType.BOOLEAN,
                    "True if good for inference (no coverage on device with proprietary traffic)",
                ),
            ),
            value_contains_null=True,
        ),
        nullable=False,
        metadata=Metadata(
            comment="Simplified signal coverage map with rule counts for easier debugging. "
            "Map keys are signal types (e.g., 'odometer', 'fuel_level_percent'). "
            "Values contain coverage status, rule counts, and ML candidacy flags."
        ),
    ),
    # Coverage summary statistics
    Column(
        name="coverage_gap_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of signal types that are not covered (month_covered=false). Higher values indicate more reverse engineering opportunities."
        ),
    ),
    Column(
        name="covered_signal_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of signal types that are covered (month_covered=true)."
        ),
    ),
    Column(
        name="total_signals_evaluated",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total count of signal types evaluated for coverage on this device."
        ),
    ),
    Column(
        name="signals_with_decoding_rules",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of signal types that have available decoding rules from dim_combined_signal_definitions for this vehicle type."
        ),
    ),
    # Candidate type summary statistics (updated per user requirements)
    Column(
        name="training_candidate_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of signal types that are training candidates (any device that has coverage)"
        ),
    ),
    Column(
        name="preferred_training_candidate_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of signal types that are preferred training candidates (has coverage + THAT signal's obd_value is broadcast + non-standard)"
        ),
    ),
    Column(
        name="inference_candidate_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of signal types that are inference candidates (no coverage on devices with non-standard traffic - device-level traffic detection applied to all signals)"
        ),
    ),
    Column(
        name="no_promotion_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of signal types that have no available promotions/decoding rules"
        ),
    ),
    # Training and inference candidate classification (updated per user requirements)
    Column(
        name="is_training_candidate",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this trace is a valid training candidate (any device that has coverage - training_candidate_count > 0)"
        ),
    ),
    Column(
        name="is_preferred_training_candidate",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this trace is a preferred training candidate (has signals with coverage + THAT signal's obd_value is broadcast + non-standard - preferred_training_candidate_count > 0)"
        ),
    ),
    Column(
        name="is_inference_candidate",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this trace is a valid inference candidate (has uncovered signals on a device with non-standard traffic - inference_candidate_count > 0)"
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Identifies CAN trace recording windows with enhanced broadcast/standard-aware candidate classification "
        "for targeted reverse engineering. Integrates coverage analysis with broadcast/standard signal characteristics from "
        "dim_combined_signal_definitions. Three candidate types: (1) Training - any device with coverage, (2) Preferred Training - "
        "coverage + broadcast + non-standard rules (ideal for ML training), (3) Inference - no coverage on devices with actual non-standard broadcast "
        "traffic available to analyze. Device-level non-standard traffic detection is applied to all signals on that device for inference candidacy.",
        row_meaning="Each row represents a specific CAN recording window eligible for reverse engineering, with precise timing details "
        "(start/end times, duration, CAN interfaces), enhanced signal coverage mapping that includes coverage status and decoding rule "
        "availability, and boolean training/inference candidacy flags for each signal type.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_ELIGIBLE_CAN_RECORDING_WINDOWS),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP_FULL),
        AnyUpstream(ProductAnalyticsStaging.FCT_DIAGNOSTIC_MESSAGES_CONSOLIDATED),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
        AnyUpstream(ProductAnalyticsStaging.DIM_COMBINED_SIGNAL_DEFINITIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_REVERSE_ENGINEERING_TRACE_CANDIDATES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_reverse_engineering_trace_candidates(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
