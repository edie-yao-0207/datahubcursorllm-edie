"""
Pre-Resolved CAN Classifications Per Trace with Signal Definition Overrides

This asset pre-computes CAN ID to transport protocol classifications for each individual trace,
eliminating the complex resolution logic that was previously done in fct_can_trace_recompiled.
It combines vehicle-specific mappings with universal fallbacks per trace, applying proper priority.

Signal definition overrides from dim_combined_signal_definitions take precedence over pattern-based
classification to handle cases where CAN IDs fall in protocol ranges (e.g., J1939 PGN ranges) but
are actually broadcast signals (e.g., GM vehicles using J1939 CAN ID ranges without J1939 protocol).

Key benefits:
- No runtime resolution needed in consuming assets (fct_can_trace_recompiled)
- Signal definition overrides prevent misclassification of broadcast signals
- Pre-computed MAP_CONCAT resolution (vehicle-specific overrides universal)
- Single join per trace instead of complex COALESCE logic
- Better performance and consistency across consuming assets
- Eliminates duplicate resolution logic across multiple assets
- Trace-level granularity enables better debugging and monitoring
"""

from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.constants import (
    AWSRegion,
    FRESHNESS_SLO_9AM_PST,
    FIRMWAREVDP,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
    map_of,
    array_of,
    struct_of,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.firmware.can.can_recompiler import TransportProtocolType
from .dim_device_vehicle_properties import MMYEF_ID

# Enhanced CAN classification mapping with comprehensive frame processing metadata
CAN_ID_TO_CLASSIFICATION_MAP_COLUMN = Column(
    name="can_id_to_classification_map",
    type=map_of(
        DataType.LONG,  # CAN ID key
        struct_of(
            ("transport_id", DataType.INTEGER, True),
            ("is_valid_isotp", DataType.BOOLEAN, True),
            ("pgn", DataType.LONG, True),
            ("is_j1939_multiframe", DataType.BOOLEAN, True),
            ("diagnostic_role", DataType.STRING, True),
            ("total_frames", DataType.LONG, True),
            ("single_frame_count", DataType.LONG, True),
            ("valid_service_single_frame_count", DataType.LONG, True),
            ("request_single_frame_count", DataType.LONG, True),
            ("response_single_frame_count", DataType.LONG, True),
            ("first_frame_count", DataType.LONG, True),
            ("consecutive_frame_count", DataType.LONG, True),
            ("flow_control_count", DataType.LONG, True),
            ("invalid_isotp_count", DataType.LONG, True),
        ),
        value_contains_null=True,
    ),
    nullable=True,
    metadata=Metadata(
        comment="Enhanced map of CAN ID to comprehensive classification metadata. "
        "Contains transport protocol, validation flags, and processing instructions per CAN ID. "
        "Eliminates need for runtime classification logic in fct_can_trace_recompiled."
    ),
)

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.TRACE_UUID,
    MMYEF_ID,
    # Enhanced CAN classification mapping with comprehensive frame processing metadata
    CAN_ID_TO_CLASSIFICATION_MAP_COLUMN,
    Column(
        name="classification_source",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="Source of the classification: 1=vehicle_specific, 2=universal_fallback, 0=none. NULL when no classification available."
        ),
    ),
    Column(
        name="total_mappings_count",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="Total number of CAN ID classifications in the final resolved mapping. NULL when unavailable."
        ),
    ),
]

# Schema and primary keys
SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
WITH can_frames_with_headers AS (
    -- Single-pass CAN frame processing with vehicle properties
    -- Consolidates trace-vehicle mapping and header extraction for better performance
    SELECT 
        can.date,
        can.org_id,
        can.device_id,
        can.trace_uuid,
        can.id,
        vp.mmyef_id,
        -- Optimized byte extraction (avoid expensive HEX/CONV operations)
        CASE 
            WHEN can.payload IS NOT NULL AND LENGTH(can.payload) > 0 
            THEN ASCII(SUBSTRING(can.payload, 1, 1))
            ELSE NULL 
        END AS header_byte,
        CASE 
            WHEN can.payload IS NOT NULL AND LENGTH(can.payload) > 1 
            THEN ASCII(SUBSTRING(can.payload, 2, 1))
            ELSE NULL 
        END AS service_byte,
        COALESCE(LENGTH(can.payload), 0) AS payload_length
    FROM product_analytics_staging.fct_sampled_can_frames can
    LEFT JOIN {product_analytics_staging}.dim_device_vehicle_properties vp USING (date, org_id, device_id)
    WHERE can.id IS NOT NULL
    AND can.date BETWEEN '{date_start}' AND '{date_end}'
),

actual_can_ids AS (
    -- Extract unique CAN IDs with header byte analysis for ISOTP validation
    SELECT 
        date,
        org_id,
        device_id,
        trace_uuid,
        id,
        mmyef_id,
        COUNT(*) AS total_frames,
        -- Count frames by ISOTP header pattern (using pre-calculated header_byte)
        -- Single frame: upper nibble = 0 AND lower nibble 1-7 (length must be valid)
        COUNT(CASE WHEN header_byte IS NOT NULL AND (header_byte & 240) = 0 AND (header_byte & 15) BETWEEN 1 AND 7 THEN 1 END) AS single_frame_count,
        -- Count single frames with valid diagnostic service IDs
        COUNT(CASE WHEN header_byte IS NOT NULL AND (header_byte & 240) = 0 
                     AND (header_byte & 15) BETWEEN 1 AND 7 
                     AND payload_length >= 2
                     AND service_byte IS NOT NULL AND service_byte IN (
                         -- Unified diagnostic services (all treated as UDS)
                         -- Request services (0x01-0x0A, 0x10-0x3E, 0x83-0x87)
                         1, 2, 3, 4, 5, 6, 7, 8, 9, 10,                                    -- Former J1979 requests
                         16, 17, 20, 25, 34, 35, 36, 39, 40, 42, 44, 46, 47,              -- UDS requests
                         49, 52, 53, 54, 55, 56, 61, 62, 131, 132, 133, 134, 135,         -- More UDS requests
                         -- Response services (0x41-0x4A, 0x50-0x7E)
                         65, 66, 67, 68, 69, 70, 71, 72, 73, 74,                          -- Former J1979 responses
                         80, 81, 84, 89, 98, 99, 100, 103, 104, 106, 108, 110, 111,      -- UDS positive responses
                         113, 116, 117, 118, 119, 120, 125, 126                          -- More UDS responses
                     ) THEN 1 END) AS valid_service_single_frame_count,
        -- Count request frames
        COUNT(CASE WHEN header_byte IS NOT NULL AND (header_byte & 240) = 0 
                     AND (header_byte & 15) BETWEEN 1 AND 7 
                     AND payload_length >= 2
                     AND service_byte IS NOT NULL AND service_byte IN (
                         -- Unified diagnostic request services (all treated as UDS)
                         1, 2, 3, 4, 5, 6, 7, 8, 9, 10,                                    -- Former J1979 requests
                         16, 17, 20, 25, 34, 35, 36, 39, 40, 42, 44, 46, 47,              -- UDS requests
                         49, 52, 53, 54, 55, 56, 61, 62, 131, 132, 133, 134, 135          -- More UDS requests
                     ) THEN 1 END) AS request_single_frame_count,
        -- Count response frames
        COUNT(CASE WHEN header_byte IS NOT NULL AND (header_byte & 240) = 0 
                     AND (header_byte & 15) BETWEEN 1 AND 7 
                     AND payload_length >= 2
                     AND service_byte IS NOT NULL AND service_byte IN (
                         -- Unified diagnostic response services (all treated as UDS)
                         65, 66, 67, 68, 69, 70, 71, 72, 73, 74,                          -- Former J1979 responses
                         80, 81, 84, 89, 98, 99, 100, 103, 104, 106, 108, 110, 111,      -- UDS positive responses
                         113, 116, 117, 118, 119, 120, 125, 126                          -- More UDS responses
                     ) THEN 1 END) AS response_single_frame_count,
        -- ISOTP frame type counts (much cleaner now!)
        COUNT(CASE WHEN header_byte IS NOT NULL AND (header_byte & 240) = 16 THEN 1 END) AS first_frame_count,
        COUNT(CASE WHEN header_byte IS NOT NULL AND (header_byte & 240) = 32 THEN 1 END) AS consecutive_frame_count,
        -- Flow control: validation for genuine ISOTP flow control
        COUNT(CASE WHEN header_byte IS NOT NULL AND (header_byte & 240) = 48 AND (header_byte & 15) BETWEEN 0 AND 2 
                     AND payload_length >= 3
                     AND service_byte IS NOT NULL AND (
                         -- Continue (0x30): second byte is block size (0 = no limit, 1-255 = block size)
                         (header_byte = 48 AND service_byte BETWEEN 0 AND 255) OR
                         -- Wait (0x31): second byte typically 0x00 
                         (header_byte = 49 AND service_byte = 0) OR
                         -- Overflow/Abort (0x32): second byte typically 0x00
                         (header_byte = 50 AND service_byte = 0)
                     ) THEN 1 END) AS flow_control_count,
        -- Count invalid ISOTP-like frames (look like ISOTP but don't match valid patterns)
        COUNT(CASE WHEN header_byte IS NOT NULL AND (header_byte & 240) = 48 AND (
                         -- Invalid flow control type (not 0-2)
                         (header_byte & 15) NOT BETWEEN 0 AND 2 OR
                         -- Valid flow control type but invalid payload pattern
                         ((header_byte & 15) BETWEEN 0 AND 2 AND NOT (
                             payload_length >= 3 AND service_byte IS NOT NULL AND (
                                 (header_byte = 48 AND service_byte BETWEEN 0 AND 255) OR
                                 (header_byte = 49 AND service_byte = 0) OR
                                 (header_byte = 50 AND service_byte = 0)
                             )
                         ))
                     ) THEN 1 END) AS invalid_isotp_count,
        -- Sample first header byte for additional validation
        FIRST(header_byte) AS sample_header_byte
    FROM can_frames_with_headers
    GROUP BY date, org_id, device_id, trace_uuid, id, mmyef_id
),

signal_definition_overrides AS (
    -- Get broadcast signal definitions per MMYEF_ID that can override derived classifications
    -- This handles cases like GM vehicles using J1939 CAN ID ranges but not running J1939 protocol
    SELECT 
        mmyef_id,
        response_id,
        application_id,
        is_broadcast
    FROM {product_analytics_staging}.dim_combined_signal_definitions
    WHERE is_broadcast = TRUE
      AND response_id IS NOT NULL
      AND mmyef_id IS NOT NULL
    GROUP BY mmyef_id, response_id, application_id, is_broadcast
),

can_ids_with_fields AS (
    -- Pre-calculate CAN ID fields and classifications for comprehensive metadata
    SELECT 
        date,
        org_id,
        device_id,
        trace_uuid,
        mmyef_id,
        id,
        total_frames,
        single_frame_count,
        valid_service_single_frame_count,
        request_single_frame_count,
        response_single_frame_count,
        first_frame_count,
        consecutive_frame_count,
        flow_control_count,
        invalid_isotp_count,
        sample_header_byte,
        -- Extract 29-bit extended CAN ID fields
        (id >> 26) & 7 AS priority,
        (id >> 25) & 1 AS reserved_bit,
        (id >> 16) & 255 AS pf,
        (id >> 8) & 255 AS ps,
        id & 255 AS sa,
        -- Calculate J1939 PGN (only for 29-bit extended IDs, NULL for 11-bit standard IDs)
        CASE 
            WHEN id > 2047 THEN  -- 29-bit extended ID (J1939)
                CASE WHEN ((id >> 16) & 255) >= 240 
                    THEN (((id >> 16) & 255) << 8) | ((id >> 8) & 255)
                    ELSE ((id >> 16) & 255) << 8 
                END
            ELSE NULL  -- 11-bit standard ID (ISOTP) - PGN not applicable
        END AS pgn,
        -- ISOTP header validation - check if headers are consistent and valid ISOTP patterns
        CASE 
            WHEN total_frames > 0 THEN
                CASE 
                    WHEN (single_frame_count + first_frame_count + consecutive_frame_count + flow_control_count) > 0 AND
                         -- Require high percentage of frames to have ISOTP headers (>= 80%)
                         (single_frame_count + first_frame_count + consecutive_frame_count + flow_control_count) >= (total_frames * 0.8) AND
                         -- Reject if too many invalid ISOTP-like frames (> 20% of total)
                         invalid_isotp_count <= (total_frames * 0.2) AND
                         -- Additional validation: single frames should have valid length in lower nibble (1-7)
                         (single_frame_count = 0 OR (sample_header_byte & 15) BETWEEN 1 AND 7) AND
                         -- ISOTP role-based validation: each CAN ID should have consistent role patterns
                         (
                             -- Pure single-frame communication with valid service IDs
                             (single_frame_count >= (total_frames * 0.8) AND valid_service_single_frame_count >= (single_frame_count * 0.8)) OR
                             -- Mixed single-frame + flow control pattern (diagnostic request/response with flow control)
                             (single_frame_count > 0 AND flow_control_count > 0 AND 
                              valid_service_single_frame_count >= (single_frame_count * 0.8) AND
                              (single_frame_count + flow_control_count) >= (total_frames * 0.8)) OR
                             -- Requester multiframe pattern: first frame + consecutive frames (minimal flow control)
                             (first_frame_count > 0 AND consecutive_frame_count > 0 AND flow_control_count <= (total_frames * 0.2)) OR
                             -- Responder pattern: flow control frames WITH corresponding multiframe data
                             (flow_control_count > 0 AND (first_frame_count > 0 OR consecutive_frame_count > 0) AND 
                              consecutive_frame_count <= (total_frames * 0.3)) OR
                             -- Pure flow control pattern (diagnostic tool managing multiframe responses)
                             (flow_control_count >= (total_frames * 0.8) AND single_frame_count = 0 AND 
                              first_frame_count = 0 AND consecutive_frame_count = 0) OR
                             -- Pure first frame pattern (ECU starting multiframe responses)
                             (first_frame_count >= (total_frames * 0.8) AND single_frame_count = 0 AND 
                              flow_control_count = 0 AND consecutive_frame_count <= (total_frames * 0.2))
                         )
                    THEN TRUE
                    ELSE FALSE
                END
            ELSE FALSE
        END AS has_valid_isotp_headers
    FROM actual_can_ids
),

base_classifications AS (
    -- Apply comprehensive classification logic with validation and signal definition overrides
    SELECT 
        cf.date,
        cf.org_id,
        cf.device_id,
        cf.trace_uuid,
        cf.mmyef_id,
        cf.id,
        cf.pgn,
        cf.total_frames,
        cf.single_frame_count,
        cf.valid_service_single_frame_count,
        cf.request_single_frame_count,
        cf.response_single_frame_count,
        cf.first_frame_count,
        cf.consecutive_frame_count,
        cf.flow_control_count,
        cf.invalid_isotp_count,
        sdo.application_id AS override_application_id,
        CASE 
            -- OVERRIDE LAYER: If broadcast signal definition exists for this MMYEF_ID + CAN ID combination
            -- Priority 1: Check for signal definition override first
            WHEN sdo.application_id IS NOT NULL THEN
                CASE 
                    -- application_id = 0 (NONE) → broadcast/no protocol → TRANSPORT_NONE
                    WHEN sdo.application_id = 0 THEN {TRANSPORT_NONE}
                    -- application_id = 1 (UDS) → diagnostic protocol → TRANSPORT_ISOTP
                    WHEN sdo.application_id = 1 THEN {TRANSPORT_ISOTP}
                    -- application_id = 3 (J1939) → J1939 protocol → TRANSPORT_J1939
                    WHEN sdo.application_id = 3 THEN {TRANSPORT_J1939}
                    -- Unknown application_id → fall back to derived classification
                    ELSE NULL
                END
            ELSE NULL
        END AS override_transport_id,
        CASE 
            -- ISOTP detection - 11-bit standard IDs WITH payload validation
            WHEN cf.id <= 2047 AND (
                cf.id = 2015 OR                    -- Diagnostic broadcast (0x7DF)
                cf.id BETWEEN 2016 AND 2023 OR    -- Diagnostic request range (0x7E0-0x7E7)
                cf.id BETWEEN 2024 AND 2031 OR    -- Diagnostic response range (0x7E8-0x7EF)
                cf.id BETWEEN 1792 AND 2047       -- Extended diagnostic range (0x700-0x7FF)
            ) AND cf.has_valid_isotp_headers = TRUE THEN {TRANSPORT_ISOTP}
            
            -- J1939 detection - 29-bit extended IDs with ISOTP tie-breaker
            WHEN cf.id > 2047 AND 
                cf.reserved_bit = 0 AND           -- Reserved bit must be 0
                cf.pf NOT IN (218, 219) AND       -- Exclude known ISOTP PF values (0xDA, 0xDB)
                (
                    cf.pgn IN (60160, 60416, 59904) OR          -- Transport Protocol PGNs: TP.DT, TP.CM, Request
                    cf.pgn BETWEEN 1 AND 61439 OR               -- Standard J1939 (0x0001-0xEFFF)
                    cf.pgn BETWEEN 61440 AND 61695 OR           -- Diagnostic (0xF000-0xF0FF)
                    cf.pgn BETWEEN 65024 AND 65279 OR           -- Normal (0xFE00-0xFEFF)
                    cf.pgn BETWEEN 65280 AND 65535 OR           -- Proprietary (0xFF00-0xFFFF)
                    cf.pf >= 240                                 -- Catch-all for high PF values
                ) AND 
                -- TIE-BREAKER: Only apply to narrow PGN range where GM proprietary ISOTP is confirmed
                -- For PGNs outside the conflict range (1-20000), always classify as J1939
                (cf.pgn NOT BETWEEN 1 AND 20000 OR  -- PGNs outside conflict range → always J1939
                 NOT (
                    -- VERY STRICT: Only trigger for clear diagnostic ISOTP with multiframe + valid services
                    -- AND only in the remaining narrow PGN ranges (mostly 1-61439 standard range)
                    (cf.first_frame_count > 0 AND cf.consecutive_frame_count > 0 AND 
                     cf.valid_service_single_frame_count > 0 AND
                     -- Must have significant diagnostic activity (not just random data)
                     (cf.valid_service_single_frame_count >= (cf.single_frame_count * 0.8) OR
                      (cf.first_frame_count + cf.consecutive_frame_count) >= (cf.total_frames * 0.8)))
                )) THEN {TRANSPORT_J1939}
            
            -- ISOTP detection - 29-bit extended IDs WITH payload validation
            WHEN cf.id > 2047 AND (
                cf.pf = 218 OR                                 -- PF = 0xDA (classic diagnostic)
                (cf.pf = 219 AND cf.sa = 241) OR                 -- PF = 0xDB with SA = 0xF1 (functional diagnostic)
                (cf.priority = 5 AND cf.pf IN (252, 253, 254))   -- Priority 5, PF in [0xFC, 0xFD, 0xFE] (WWH-OBD)
            ) AND cf.has_valid_isotp_headers = TRUE THEN {TRANSPORT_ISOTP}
            
            -- ISOTP detection - GM proprietary and other non-standard ISOTP (tie-breaker winner)
            WHEN cf.id > 2047 AND 
                -- Only allow tie-breaker for very narrow PGN range where GM proprietary ISOTP is confirmed
                (cf.pgn BETWEEN 1 AND 20000) AND  -- Only allow very specific low PGN range
                (
                    -- VERY STRICT: Only for clear diagnostic ISOTP with overwhelming evidence
                    (cf.first_frame_count > 0 AND cf.consecutive_frame_count > 0 AND 
                     cf.valid_service_single_frame_count > 0 AND
                     -- Must have significant diagnostic activity (not just random data)
                     (cf.valid_service_single_frame_count >= (cf.single_frame_count * 0.8) OR
                      (cf.first_frame_count + cf.consecutive_frame_count) >= (cf.total_frames * 0.8)))
                ) THEN {TRANSPORT_ISOTP}
            
            -- Default to NONE if no patterns match
            ELSE {TRANSPORT_NONE}
        END as derived_transport_id
    FROM can_ids_with_fields cf
    LEFT JOIN signal_definition_overrides sdo 
        ON cf.mmyef_id = sdo.mmyef_id 
        AND cf.id = sdo.response_id
),

base_classifications_with_final_transport AS (
    -- Select final transport_id: override if available, otherwise derived
    SELECT 
        date,
        org_id,
        device_id,
        trace_uuid,
        mmyef_id,
        id,
        pgn,
        total_frames,
        single_frame_count,
        valid_service_single_frame_count,
        request_single_frame_count,
        response_single_frame_count,
        first_frame_count,
        consecutive_frame_count,
        flow_control_count,
        invalid_isotp_count,
        COALESCE(override_transport_id, derived_transport_id) AS base_transport_id
    FROM base_classifications
),

enhanced_classifications AS (
    -- Add comprehensive metadata for each CAN ID classification
    SELECT
        date,
        org_id,
        device_id, 
        trace_uuid,
        mmyef_id,
        id,
        base_transport_id,
        pgn,
        total_frames,
        single_frame_count,
        valid_service_single_frame_count,
        request_single_frame_count,
        response_single_frame_count,
        first_frame_count,
        consecutive_frame_count,
        flow_control_count,
        invalid_isotp_count,
        
        -- ISOTP validation and metadata
        CASE 
            WHEN base_transport_id = {TRANSPORT_ISOTP} THEN TRUE
            ELSE NULL
        END AS is_isotp,
        
        -- Request/Response classification for diagnostic communication
        CASE 
            WHEN base_transport_id = {TRANSPORT_ISOTP} THEN
                CASE 
                    -- Pure request patterns: single-frame requests OR requests + flow control
                    WHEN request_single_frame_count > 0 AND response_single_frame_count = 0 THEN 'REQUEST'
                    -- Pure response patterns: single-frame responses OR responses + flow control  
                    WHEN response_single_frame_count > 0 AND request_single_frame_count = 0 THEN 'RESPONSE'
                    -- Mixed request/response patterns
                    WHEN request_single_frame_count > 0 AND response_single_frame_count > 0 THEN 'MIXED'
                    -- Flow control only patterns - classify based on CAN ID convention
                    WHEN flow_control_count > 0 AND single_frame_count = 0 THEN
                        CASE 
                            -- Standard OBD-II request IDs (diagnostic tool to ECU)
                            WHEN id IN (2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023) THEN 'REQUEST'  -- 0x7E0-0x7E7
                            WHEN id = 2015 THEN 'REQUEST'  -- 0x7DF (functional addressing)
                            -- Standard OBD-II response IDs (ECU to diagnostic tool)  
                            WHEN id IN (2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031) THEN 'RESPONSE' -- 0x7E8-0x7EF
                            ELSE NULL
                        END
                    ELSE NULL
                END
            ELSE NULL
        END AS diagnostic_role,
        
        -- J1939 multiframe detection (TP.CM, TP.DT, TP.ACK frames need UDF)
        CASE 
            WHEN base_transport_id = {TRANSPORT_J1939} AND pgn IN (60160, 60416, 59904) THEN TRUE
            WHEN base_transport_id = {TRANSPORT_J1939} THEN FALSE
            ELSE NULL
        END AS is_j1939_multiframe
        
    FROM base_classifications_with_final_transport
    -- Note: UDF processing requirements are determined dynamically in fct_can_trace_recompiled
    -- based on individual frame analysis (ISOTP frame types, J1939 transport protocols)
    -- Static classification here cannot accurately predict multiframe message patterns
),

final_classifications AS (
    -- Create final classification map per trace
    SELECT
        date,
        org_id,
        device_id,
        trace_uuid,
        mmyef_id,
        
        MAP_FROM_ENTRIES(
            COLLECT_LIST(
                STRUCT(
                    id AS key,
                    STRUCT(
                        base_transport_id AS transport_id,
                        is_isotp AS is_valid_isotp,  -- For now, assume ISOTP detection means valid
                        pgn,
                        is_j1939_multiframe,
                        diagnostic_role,
                        total_frames,
                        single_frame_count,
                        valid_service_single_frame_count,
                        request_single_frame_count,
                        response_single_frame_count,
                        first_frame_count,
                        consecutive_frame_count,
                        flow_control_count,
                        invalid_isotp_count
                    ) AS value
                )
            )
        ) AS can_id_to_classification_map,
        
        -- Metadata for monitoring
        1 as classification_source,  -- Enhanced logic
        CAST(COUNT(*) AS INT) as total_mappings_count
        
    FROM enhanced_classifications
    GROUP BY date, org_id, device_id, trace_uuid, mmyef_id
)

SELECT 
    date,
    org_id,
    device_id,
    trace_uuid,
    mmyef_id,
    can_id_to_classification_map,
    classification_source,
    total_mappings_count
FROM final_classifications
WHERE can_id_to_classification_map IS NOT NULL
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Enhanced CAN ID to comprehensive classification metadata per trace with signal definition overrides. "
        "This table consolidates ALL classification logic previously scattered across fct_can_trace_recompiled, "
        "including transport protocol detection, validation flags, UDF routing decisions, PGN extraction, "
        "and frame type analysis. Signal definition overrides from dim_combined_signal_definitions take precedence "
        "over pattern-based classification to handle cases where CAN IDs fall in protocol ranges (e.g., J1939) "
        "but are actually broadcast signals. Each trace gets pre-computed classification metadata eliminating expensive "
        "runtime validation and enabling simple join-based processing in downstream assets.",
        row_meaning="Each row represents a single CAN trace with its complete pre-computed CAN ID classification metadata. "
        "The classification map contains transport protocol (with signal definition overrides applied), validation flags, "
        "and processing instructions per CAN ID.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_SAMPLED_CAN_FRAMES),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
        AnyUpstream(ProductAnalyticsStaging.DIM_COMBINED_SIGNAL_DEFINITIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.MAP_CAN_CLASSIFICATIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def map_can_classifications(context: AssetExecutionContext) -> str:
    """Enhanced CAN ID classifications with comprehensive validation metadata and signal definition overrides.
    
    Signal definition overrides from dim_combined_signal_definitions take precedence over pattern-based
    classification. When a broadcast signal (is_broadcast=true) matches a CAN ID in the trace, the
    signal definition's application_id is used to determine the transport protocol:
    - application_id = 0 (NONE) → TRANSPORT_NONE (broadcast/no protocol)
    - application_id = 1 (UDS) → TRANSPORT_ISOTP (diagnostic protocol)
    - application_id = 3 (J1939) → TRANSPORT_J1939 (J1939 protocol)
    
    This prevents misclassification of GM vehicles using J1939 CAN ID ranges without J1939 protocol.
    """
    return format_date_partition_query(
        QUERY,
        context,
        TRANSPORT_ISOTP=TransportProtocolType.ISOTP.value,
        TRANSPORT_J1939=TransportProtocolType.J1939.value,
        TRANSPORT_NONE=TransportProtocolType.NONE.value,
    )
