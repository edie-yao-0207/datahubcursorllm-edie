"""
dim_combined_signal_definitions

Unified signal definitions combining all signal sources with consistent enumeration storage.
Harmonizes SPS promotions, J1939 standard signals, and passenger vehicle signals
into a comprehensive signal catalog following DataWeb enumeration storage best practices.
"""

from dataclasses import replace
from enum import IntEnum

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.can.can_recompiler.core.enums import (
    ApplicationProtocolType,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, SignalPromotionDb
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_query
from dataweb.userpkgs.utils import build_table_description
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr

from .dim_device_vehicle_properties import MMYEF_HASH_EXPRESSION, MMYEF_ID

# PopulationType constant from signalpromotion_service.proto
POPULATION_TYPE_MMYETPF = 2

# Deterministic signal catalog ID expression using xxhash64
# This ensures signal_catalog_id remains stable across runs and is unique per signal
# Includes ALL distinguishing fields (33 total) to prevent hash collisions
SIGNAL_CATALOG_ID_HASH_EXPRESSION = """CAST(XXHASH64(
    CONCAT_WS('|',
        -- Vehicle identification (7 fields)
        CAST(COALESCE(make, '') AS STRING),
        CAST(COALESCE(model, '') AS STRING),
        CAST(COALESCE(year, -1) AS STRING),
        CAST(COALESCE(engine_model, '') AS STRING),
        CAST(COALESCE(powertrain, -1) AS STRING),
        CAST(COALESCE(fuel_group, -1) AS STRING),
        CAST(COALESCE(trim, '') AS STRING),
        -- Protocol and data identification (5 fields)
        CAST(COALESCE(protocol_id, -1) AS STRING),
        CAST(COALESCE(application_id, -1) AS STRING),
        CAST(COALESCE(data_identifier, -1) AS STRING),
        CAST(COALESCE(stream_id, -1) AS STRING),
        CAST(COALESCE(obd_value, -1) AS STRING),
        -- Protocol-specific identifiers (6 fields)
        CAST(COALESCE(pgn, -1) AS STRING),
        CAST(COALESCE(spn, -1) AS STRING),
        CAST(COALESCE(response_id, -1) AS STRING),
        CAST(COALESCE(request_id, -1) AS STRING),
        CAST(COALESCE(passive_response_mask, -1) AS STRING),
        CAST(COALESCE(request_period_ms, -1) AS STRING),
        -- Signal position and encoding (4 fields)
        CAST(COALESCE(bit_start, -1) AS STRING),
        CAST(COALESCE(bit_length, -1) AS STRING),
        CAST(COALESCE(endian, -1) AS STRING),
        CAST(COALESCE(sign, -1) AS STRING),
        -- Signal transformation parameters (6 fields)
        CAST(COALESCE(scale, -1) AS STRING),
        CAST(COALESCE(offset, -1) AS STRING),
        CAST(COALESCE(minimum, -1) AS STRING),
        CAST(COALESCE(maximum, -1) AS STRING),
        CAST(COALESCE(internal_scaling, -1) AS STRING),
        CAST(COALESCE(unit, '') AS STRING),
        -- Signal metadata (2 fields)
        CAST(COALESCE(description, '') AS STRING),
        CAST(COALESCE(mapping, '') AS STRING),
        -- Source identification (3 fields)
        CAST(COALESCE(source_id, -1) AS STRING),
        CAST(COALESCE(data_source_id, -1) AS STRING),
        CAST(COALESCE(signal_uuid, '') AS STRING)
    )
) AS BIGINT)"""


class SignalSourceType(IntEnum):
    """Signal source enum - indicates which table/system the signal originated from."""

    INVALID = 0  # Invalid/unknown source
    GLOBAL = 1  # Global signal definitions
    J1979_DA = 2  # J1979 Digital Annex signals
    J1939_DA = 3  # J1939 Digital Annex signals
    SPS = 4  # Signal Promotion System signals
    AI = 5  # AI/ML promoted signals
    PROMOTION_GAP = 6  # Telematics promotion gap signals


# Common signal-related column definitions (shared with other signal assets)
SIGNAL_CATALOG_ID_COLUMN_BASE = Column(
    name="signal_catalog_id",
    type=DataType.LONG,
    nullable=True,
    metadata=Metadata(
        comment="Unique numeric identifier for each signal catalog entry."
    ),
)

COLUMNS = [
    replace(SIGNAL_CATALOG_ID_COLUMN_BASE, nullable=False, primary_key=True),
    replace(ColumnType.DATA_IDENTIFIER.value, nullable=True),
    replace(ColumnType.MAKE.value, nullable=True),
    replace(ColumnType.MODEL.value, nullable=True),
    replace(ColumnType.YEAR.value, nullable=True),
    replace(ColumnType.ENGINE_MODEL.value, nullable=True),
    replace(ColumnType.POWERTRAIN.value, nullable=True),
    replace(ColumnType.FUEL_GROUP.value, nullable=True),
    replace(ColumnType.TRIM.value, nullable=True),
    MMYEF_ID,
    replace(ColumnType.OBD_VALUE.value, nullable=True),
    ColumnType.BIT_START,
    ColumnType.BIT_LENGTH,
    ColumnType.ENDIAN,
    ColumnType.SCALE,
    ColumnType.INTERNAL_SCALING,
    ColumnType.SIGN,
    ColumnType.OFFSET,
    ColumnType.MINIMUM,
    ColumnType.MAXIMUM,
    ColumnType.UNIT,
    ColumnType.DESCRIPTION,
    ColumnType.MAPPING,
    ColumnType.PGN,
    ColumnType.SPN,
    ColumnType.PASSIVE_RESPONSE_MASK,
    ColumnType.RESPONSE_ID,
    ColumnType.REQUEST_ID,
    ColumnType.SOURCE_ID,
    ColumnType.APPLICATION_ID,
    ColumnType.DATA_SOURCE_ID,
    ColumnType.PROTOCOL_ID,
    ColumnType.SIGNAL_UUID,
    ColumnType.REQUEST_PERIOD_MS,
    ColumnType.IS_BROADCAST,
    ColumnType.IS_STANDARD,
    replace(ColumnType.STREAM_ID.value, nullable=False),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)


def _build_j1939_signals_query() -> str:
    """Build J1939 standard signals query."""
    j1939_query = """
    SELECT
        j1939.pgn AS data_identifier,
        NULL AS make,
        NULL AS model,
        NULL AS year,
        NULL AS engine_model,
        NULL AS powertrain,
        NULL AS fuel_group,
        NULL AS trim,
        j1939.obd_value,
        j1939.bit_start,
        j1939.bit_length,
        j1939.endian,
        j1939.scale,
        j1939.internal_scaling,
        j1939.sign,
        j1939.offset,
        j1939.minimum,
        j1939.maximum,
        j1939.unit,
        CONCAT(
            COALESCE(j1939.pgn_description, ''),
            ' ',
            COALESCE(j1939.spn_description, '')
        ) AS description,
        j1939.mapping,
        j1939.pgn,
        j1939.spn,
        CAST(NULL AS LONG) AS passive_response_mask,
        CAST(NULL AS LONG) AS response_id,
        CAST(NULL AS LONG) AS request_id,
        CAST(6 AS INTEGER) AS source_id,
        CAST({j1939_da_source} AS INTEGER) AS data_source_id,
        CAST(3 AS INTEGER) AS application_id,
        CAST(2 AS LONG) AS protocol_id,
        NULL AS signal_uuid,
        -- J1939-DA: No request_period_ms available, assume NULL for now
        -- TODO: Get request_period_ms information from DA team
        CAST(NULL AS LONG) AS request_period_ms,
        -- J1939-DA: Assume all signals are broadcast-based per user requirements
        TRUE AS is_broadcast,
        -- J1939-DA: All signals are standard (SAE J1939 Digital Annex)
        -- Even PGNs in "proprietary" ranges are standardized when they appear in J1939-DA
        TRUE AS is_standard
    FROM {product_analytics_staging}.dim_j1939_signal_definitions j1939
    """

    return format_query(j1939_query, j1939_da_source=SignalSourceType.J1939_DA)


def _build_sps_signals_query() -> str:
    """Build SPS promoted signals query.

    Protocol detection prioritizes explicit protocol=2 for J1939 signals,
    but uses pattern-based inference for other protocols.
    """
    sps_query = """
    WITH sps_with_signal_analysis AS (
        SELECT
            signals.request_period_ms,
            signals.signal_uuid,
            -- Convert data_identifier = 0 to NULL early, before application calculation
            CASE
                WHEN signals.data_identifier = 0 AND signals.passive_response_mask IS NOT NULL THEN NULL
                ELSE signals.data_identifier
            END AS data_identifier,
            signals.obd_value,
            signals.bit_start,
            signals.bit_length,
            signals.endianness,
            signals.scale,
            signals.internal_scaling,
            signals.sign,
            signals.offset,
            signals.minimum,
            signals.maximum,
            signals.comment,
            signals.passive_response_mask,
            signals.response_id,
            signals.request_id,
            signals.data_source,
            signals.protocol,
            populations.make,
            populations.model,
            populations.year,
            populations.engine_model,
            populations.powertrain,
            populations.fuel_group,
            populations.trim,

            -- Shared intermediate calculations
            -- Extract PGN from J1939 CAN ID when data_identifier is null
            -- Only extract if response_id is in valid J1939 29-bit CAN ID range (0x18000000+)
            CASE
                WHEN signals.data_identifier IS NULL
                 AND signals.response_id IS NOT NULL
                 AND signals.response_id >= 402653184  -- 0x18000000 (typical J1939 range start)
                THEN ((signals.response_id >> 8) & 65535)
                ELSE NULL
            END AS extracted_pgn,

            -- Extract UDS service from data_identifier
            CASE
                WHEN signals.data_identifier IS NOT NULL
                THEN (signals.data_identifier >> 8)
                ELSE NULL
            END AS uds_service_8bit,

            CASE
                WHEN signals.data_identifier IS NOT NULL
                THEN (signals.data_identifier >> 16)
                ELSE NULL
            END AS uds_service_16bit

        FROM signalpromotiondb.promotions
        JOIN signalpromotiondb.signals USING (signal_uuid)
        JOIN signalpromotiondb.populations USING (population_uuid)
        WHERE promotions.stage = 4
          AND promotions.status = 1
          AND populations.type = {population_type_mmyetpf}
    ),

    sps_with_application AS (
        SELECT *,
            CASE
                -- J1939 Detection: Explicit protocol field (most reliable)
                -- SPS protocol=2 indicates J1939 configuration
                WHEN s.protocol = 2 THEN {application_j1939}

                -- UDS Detection: All diagnostic services (unified J1979/UDS approach)
                -- Classic OBD-II ranges now classified as UDS for consistency
                WHEN (s.data_identifier BETWEEN 256 AND 2815)     -- 0x0100-0x0AFF (requests)
                  OR (s.data_identifier BETWEEN 16640 AND 19199)  -- 0x4100-0x4AFF (responses)
                THEN {application_uds}

                -- J1939 Detection: Direct PGN in data_identifier (18-bit range) - fallback pattern matching
                -- BUT exclude if request/response IDs are 11-bit CAN IDs (0-2047)
                WHEN s.data_identifier BETWEEN 0 AND 262143
                  AND (s.request_id IS NULL OR s.request_id NOT BETWEEN 0 AND 2047)
                  AND (s.response_id IS NULL OR s.response_id NOT BETWEEN 0 AND 2047)
                THEN {application_j1939}

                -- J1939 Detection: PGN extracted from response_id (18-bit range)
                -- BUT exclude if request/response IDs are 11-bit CAN IDs (0-2047)
                WHEN s.extracted_pgn BETWEEN 0 AND 262143
                  AND (s.request_id IS NULL OR s.request_id NOT BETWEEN 0 AND 2047)
                  AND (s.response_id IS NULL OR s.response_id NOT BETWEEN 0 AND 2047)
                THEN {application_j1939}

                -- UDS Detection: Service patterns
                WHEN s.uds_service_8bit = 33 THEN {application_uds}  -- Service 21
                WHEN s.uds_service_16bit = 34 THEN {application_uds} -- Service 22
                WHEN s.uds_service_16bit BETWEEN 1 AND 127 THEN {application_uds} -- Other UDS services

                -- UDS Detection: UDS over J1939 address ranges (specific known ranges)
                WHEN s.response_id BETWEEN 4160749568 AND 4160815103 THEN {application_uds}  -- 0xF8000000-0xF800FFFF
                WHEN s.response_id = 4161277937 THEN {application_uds}  -- 0xF80E0571

                -- Default: If none of the above patterns match
                ELSE {application_none}
            END AS calculated_application_id
        FROM sps_with_signal_analysis s
    )

    SELECT
        CAST(s.data_identifier AS LONG) AS data_identifier,
        UPPER(s.make) AS make,
        UPPER(s.model) AS model,
        CAST(s.year AS LONG) AS year,
        UPPER(s.engine_model) AS engine_model,
        CAST(s.powertrain AS INTEGER) AS powertrain,
        CAST(s.fuel_group AS INTEGER) AS fuel_group,
        UPPER(s.trim) AS trim,
        s.obd_value,
        s.bit_start,
        s.bit_length,
        s.endianness AS endian,
        s.scale,
        s.internal_scaling,
        s.sign,
        s.offset,
        s.minimum,
        s.maximum,
        NULL AS unit,
        s.comment AS description,
        NULL AS mapping,
        NULL AS pgn,
        NULL AS spn,
        CAST(s.passive_response_mask AS LONG) AS passive_response_mask,
        CAST(s.response_id AS LONG) AS response_id,
        CAST(s.request_id AS LONG) AS request_id,
        s.data_source AS source_id,

        -- From can_decoder/processor.py ApplicationProtocolType
        CAST({sps_source} AS INTEGER) AS data_source_id,
        s.calculated_application_id AS application_id,
        s.protocol AS protocol_id,
        s.signal_uuid,

        -- SPS: Get request_period_ms from signals table (consistent source)
        s.request_period_ms AS request_period_ms,

        -- SPS: Broadcast logic based on request_period_ms
        s.request_period_ms IS NULL OR s.request_period_ms = 0 AS is_broadcast,

        -- SPS: Standard signal detection using calculated application ID and context
        CASE
            -- Signals from J1979-DA (Digital Annex) are curated standard definitions
            WHEN s.data_source = {j1979_da_source} THEN TRUE
            -- Standard UDS/OBD-II signals with standard DID/PID ranges (ISO 14229, ISO 27145, SAE J1979)
            -- Some promotions in global_obd_configuration.go are standard
            WHEN s.calculated_application_id = {application_uds} AND (
                -- Service 0x22 (ReadDataByIdentifier): Standard 2-byte DIDs in 0xF1xx-0xF4xx ranges
                -- Extract service (upper 16 bits shifted) and check DID (lower 16 bits)
                ((s.data_identifier >> 16) = 34 AND (s.data_identifier & 65535) BETWEEN 61696 AND 61823)   -- Service 0x22, DID 0xF100-0xF17F
                OR ((s.data_identifier >> 16) = 34 AND (s.data_identifier & 65535) BETWEEN 61824 AND 61839)  -- Service 0x22, DID 0xF180-0xF18F
                OR ((s.data_identifier >> 16) = 34 AND (s.data_identifier & 65535) BETWEEN 61840 AND 61935)  -- Service 0x22, DID 0xF190-0xF1EF
                OR ((s.data_identifier >> 16) = 34 AND (s.data_identifier & 65535) BETWEEN 62464 AND 62719)  -- Service 0x22, DID 0xF400-0xF4FF
                -- J1979/OBD-II Services (0x01-0x0A): Standard 1-byte PIDs (all standard by SAE J1979 definition)
                -- 0x01: Current Data, 0x02: Freeze Frame, 0x03: DTCs, 0x04: Clear DTCs
                -- 0x05: O2 Sensor Test, 0x06: System Test, 0x07: Pending DTCs, 0x08: Control
                -- 0x09: Vehicle Info, 0x0A: Permanent DTCs
                OR ((s.data_identifier >> 8) BETWEEN 1 AND 10)  -- Services 0x01-0x0A with any PID
            ) THEN TRUE

            -- Standard J1939 signals with non-proprietary PGN ranges
            WHEN s.calculated_application_id = {application_j1939} AND (
                -- Standard J1939 SAE-assigned PGN ranges (either in data_identifier or extracted_pgn)
                COALESCE(s.data_identifier, s.extracted_pgn) BETWEEN 0 AND 61439      -- 0x0000-0xEFFF (Standard SAE)
                OR COALESCE(s.data_identifier, s.extracted_pgn) BETWEEN 65000 AND 65279 -- 0xFDE8-0xFEFF (Standard SAE: DM1, etc.)
            ) THEN TRUE

            -- Everything else (proprietary J1939 ranges, proprietary UDS DIDs, unknown protocols, non-standard signals)
            ELSE FALSE
        END AS is_standard
    FROM sps_with_application s
    """

    return format_query(
        sps_query,
        sps_source=SignalSourceType.SPS,
        j1979_da_source=SignalSourceType.J1979_DA,
        application_none=ApplicationProtocolType.NONE.value,
        application_uds=ApplicationProtocolType.UDS.value,
        application_j1939=ApplicationProtocolType.J1939.value,
        population_type_mmyetpf=POPULATION_TYPE_MMYETPF,
    )


def _build_passenger_signals_query() -> str:
    """Build passenger vehicle signals query."""
    passenger_query = """
    WITH passenger_with_signal_analysis AS (
        SELECT
            s.*,

            -- Shared intermediate calculations (same as SPS)
            -- Extract PGN from J1939 CAN ID when data_identifier is null
            -- Only extract if response_id is in valid J1939 29-bit CAN ID range (0x18000000+)
            CASE
                WHEN s.data_identifier IS NULL
                  AND s.response_id IS NOT NULL
                  AND s.response_id >= 402653184  -- 0x18000000 (typical J1939 range start)
                THEN ((s.response_id >> 8) & 65535)
                ELSE NULL
            END AS extracted_pgn,

            -- Extract UDS service from data_identifier
            CASE
                WHEN s.data_identifier IS NOT NULL THEN (s.data_identifier >> 8)
                ELSE NULL
            END AS uds_service_8bit,

            CASE
                WHEN s.data_identifier IS NOT NULL THEN (s.data_identifier >> 16)
                ELSE NULL
            END AS uds_service_16bit

        FROM {product_analytics_staging}.dim_combined_passenger_signal_definitions s
    ),

    passenger_with_application AS (
        SELECT *,
            CASE
                -- UDS Detection: All diagnostic services (unified J1979/UDS approach)
                -- Classic OBD-II ranges now classified as UDS for consistency
                WHEN (s.data_identifier BETWEEN 256 AND 2815)     -- 0x0100-0x0AFF (requests)
                  OR (s.data_identifier BETWEEN 16640 AND 19199)  -- 0x4100-0x4AFF (responses)
                THEN {application_uds}

                -- UDS Detection: Service patterns
                WHEN s.uds_service_8bit = 33 THEN {application_uds}  -- Service 21
                WHEN s.uds_service_16bit = 34 THEN {application_uds} -- Service 22
                WHEN s.uds_service_16bit BETWEEN 1 AND 127 THEN {application_uds} -- Other UDS services

                -- UDS Detection: UDS over J1939 address ranges (specific known ranges)
                WHEN s.response_id BETWEEN 4160749568 AND 4160815103 THEN {application_uds}  -- 0xF8000000-0xF800FFFF
                WHEN s.response_id = 4161277937 THEN {application_uds}  -- 0xF80E0571

                -- Default: If none of the above patterns match
                ELSE {application_none}
            END AS calculated_application_id
        FROM passenger_with_signal_analysis s
    )

    SELECT DISTINCT
        s.data_identifier,
        UPPER(s.make) AS make,
        UPPER(s.model) AS model,
        s.year,
        UPPER(s.engine_model) AS engine_model,
        s.powertrain,
        s.fuel_group,
        s.trim,
        s.obd_value,
        s.bit_start,
        s.bit_length,
        s.endian,
        s.scale,
        s.internal_scaling,
        s.sign,
        s.offset,
        s.minimum,
        s.maximum,
        s.unit,
        s.comment AS description,
        CAST(NULL AS STRING) AS mapping,
        CAST(NULL AS INTEGER) AS pgn,
        CAST(NULL AS INTEGER) AS spn,
        s.passive_response_mask,

        -- Standard requests sometimes have a request_id but no response_id since we
        -- accept any response_id. This is showing up as 0 in the underlying table.
        CAST(
            CASE
                WHEN s.response_id = 0 THEN NULL
                ELSE s.response_id
            END AS LONG
        ) AS response_id,
        CAST(
            CASE
                WHEN s.request_id = 0 THEN NULL
                ELSE s.request_id
            END AS LONG
        ) AS request_id,

        -- TODO: there is a source_id column in global_obd_configuration.go that we should use here
        CAST(NULL AS INTEGER) AS source_id,

        s.data_source_id,

        -- Use shared signal analysis for application detection
        s.calculated_application_id AS application_id,

        -- Protocol ID based on calculated application (for backward compatibility)
        CASE
            WHEN s.calculated_application_id = {application_uds} THEN CAST(3 AS LONG)
            ELSE CAST(0 AS LONG)
        END AS protocol_id,
        CAST(NULL AS STRING) AS signal_uuid,

        -- Passenger: No request_period_ms available in current table
        -- TODO: Add request_period_ms to passenger signals table
        CAST(NULL AS LONG) AS request_period_ms,

        -- Passenger: Broadcast logic based on signal characteristics
        CASE
            -- If has request_id AND response_id, it's request-based regardless of protocol
            WHEN s.request_id IS NOT NULL AND s.response_id IS NOT NULL
            THEN FALSE

            -- If no data_identifier, it's likely broadcast (no specific data being requested)
            WHEN s.data_identifier IS NULL
            THEN TRUE

            -- Default: request-based for UDS/J1979 with specific data identifiers
            ELSE FALSE
        END AS is_broadcast,

        -- Passenger: Standard signal detection using calculated application ID and DID/PID ranges
        CASE
            -- Standard UDS/OBD-II signals with standard DID/PID ranges (ISO 14229, ISO 27145, SAE J1979)
            WHEN s.calculated_application_id = {application_uds} AND (
                -- Signals from J1979-DA (Digital Annex) are curated standard definitions
                s.data_source_id = {j1979_da_source}
                -- Service 0x22 (ReadDataByIdentifier): Standard 2-byte DIDs in 0xF1xx-0xF4xx ranges
                OR ((s.data_identifier >> 16) = 34 AND (s.data_identifier & 65535) BETWEEN 61696 AND 61823)   -- Service 0x22, DID 0xF100-0xF17F
                OR ((s.data_identifier >> 16) = 34 AND (s.data_identifier & 65535) BETWEEN 61824 AND 61839)  -- Service 0x22, DID 0xF180-0xF18F
                OR ((s.data_identifier >> 16) = 34 AND (s.data_identifier & 65535) BETWEEN 61840 AND 61935)  -- Service 0x22, DID 0xF190-0xF1EF
                OR ((s.data_identifier >> 16) = 34 AND (s.data_identifier & 65535) BETWEEN 62464 AND 62719)  -- Service 0x22, DID 0xF400-0xF4FF
                -- J1979/OBD-II Services (0x01-0x0A): Standard 1-byte PIDs (all standard by SAE J1979 definition)
                -- 0x01: Current Data, 0x02: Freeze Frame, 0x03: DTCs, 0x04: Clear DTCs
                -- 0x05: O2 Sensor Test, 0x06: System Test, 0x07: Pending DTCs, 0x08: Control
                -- 0x09: Vehicle Info, 0x0A: Permanent DTCs
                OR ((s.data_identifier >> 8) BETWEEN 1 AND 10)  -- Services 0x01-0x0A with any PID
            ) THEN TRUE

            -- Everything else (proprietary UDS DIDs, unknown protocols, non-standard signals)
            ELSE FALSE
        END AS is_standard

    FROM passenger_with_application s
    """

    return format_query(
        passenger_query,
        j1979_da_source=SignalSourceType.J1979_DA,
        application_none=ApplicationProtocolType.NONE.value,
        application_uds=ApplicationProtocolType.UDS.value,
    )


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Unified signal definitions combining all signal sources with consistent enumeration storage. "
        "Directly queries J1939 standard signals, SPS promotion system (signalpromotiondb), and passenger vehicle "
        "signals (dim_combined_passenger_signal_definitions) to create a comprehensive signal catalog following DataWeb enumeration "
        "storage best practices (raw enum values only). Uses EndiannessType and SignType enums as the source of truth "
        "for endianness and sign interpretation across the CAN signal processing pipeline. Includes SPS promotion logic for stage 4, status 1 signals.",
        row_meaning="Each row represents a signal definition from any source (J1939, SPS promotions, or passenger signals) "
        "with a unique numeric identifier, data_source_id tracking, and raw enum values for optimal storage efficiency and consistency.",
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_J1939_SIGNAL_DEFINITIONS),
        AnyUpstream(ProductAnalyticsStaging.DIM_COMBINED_PASSENGER_SIGNAL_DEFINITIONS),
        AnyUpstream(SignalPromotionDb.PROMOTIONS),
        AnyUpstream(SignalPromotionDb.SIGNALS),
        AnyUpstream(SignalPromotionDb.POPULATIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_COMBINED_SIGNAL_DEFINITIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def dim_combined_signal_definitions(context: AssetExecutionContext) -> DataFrame:
    """
    Combine all signal sources into unified signal catalog with enumeration storage best practices.

    This asset creates a comprehensive signal catalog by combining:
    1. J1939 standard signals (generic, apply to all vehicles)
    2. SPS promoted signals (vehicle-specific, directly from signalpromotiondb)
    3. Passenger vehicle signals (vehicle-specific, from dim_combined_passenger_signal_definitions)

    The result supports efficient lookups and joins with definition tables when
    human-readable enum translations are needed for reporting or debugging.

    SPS signals are filtered to only include production-ready promotions:
    - Stage 4: Final validation complete
    - Status 1: Active/approved
    - Populations using MMYETPF population type

    Primary key generation uses ROW_NUMBER() with deterministic ordering by:
    data_identifier, make, model, year, obd_value, signal_uuid.
    """
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    query_builders = [
        ("J1939 Signals", _build_j1939_signals_query),
        ("SPS Signals", _build_sps_signals_query),
        ("Passenger Signals", _build_passenger_signals_query),
    ]

    combined_df = None
    for i, (table_name, query_builder) in enumerate(query_builders):
        context.log.info(f"Building {table_name}")

        # Get the formatted query string
        query_string = query_builder()

        context.log.info(query_string)

        # Execute the query
        table_df = spark.sql(query_string)
        context.log.info(f"{table_name} contains {table_df.count()} total signals")

        if i == 0:
            combined_df = table_df
        else:
            combined_df = combined_df.union(table_df)

    if combined_df is None:
        raise ValueError("No signal sources were processed successfully")

    # Add MMYEF_ID hash
    # From dim_vehicle_properties
    context.log.info("Adding MMYEF_ID hash for vehicle characteristics")

    # Add MMYEF_ID hash with NULL for universal signals
    combined_df = combined_df.withColumn(
        "mmyef_id",
        expr(
            f"""
        CASE
            WHEN make IS NULL AND model IS NULL AND year IS NULL
                 AND (engine_model IS NULL OR TRIM(engine_model) = '')
                 AND fuel_group IS NULL AND powertrain IS NULL
                 AND (trim IS NULL OR TRIM(trim) = '')
            THEN NULL  -- Universal signals get NULL mmyef_id
            ELSE {MMYEF_HASH_EXPRESSION.strip()}  -- Vehicle-specific signals get hash
        END
        """
        ),
    )

    # Add stream_id computation for efficient CAN trace joins
    context.log.info("Adding stream_id for direct CAN trace joins")
    from pyspark.sql.functions import when

    combined_df = combined_df.withColumn(
        "stream_id",
        when(
            col("application_id") == ApplicationProtocolType.J1939.value,
            # J1939: stream_id = XXHASH64(3, PGN) where data_identifier = PGN
            expr(
                f"XXHASH64(CAST({ApplicationProtocolType.J1939.value} AS BIGINT), CAST(COALESCE(data_identifier, -1) AS BIGINT))"
            ),
        )
        .when(
            col("application_id") == ApplicationProtocolType.UDS.value,
            # UDS: stream_id = XXHASH64(1, data_identifier) where data_identifier contains service-specific composite value
            expr(
                f"XXHASH64(CAST({ApplicationProtocolType.UDS.value} AS BIGINT), CAST(COALESCE(data_identifier, -1) AS BIGINT))"
            ),
        )
        .otherwise(
            # NONE/Regular: stream_id = XXHASH64(0, CAN_ID) where response_id is the CAN arbitration ID
            expr(
                f"XXHASH64(CAST({ApplicationProtocolType.NONE.value} AS BIGINT), CAST(COALESCE(response_id, -1) AS BIGINT))"
            )
        ),
    )

    # Add unique primary key using deterministic xxhash64
    # This ensures signal_catalog_id remains stable across runs
    context.log.info("Adding signal_catalog_id using deterministic xxhash64")
    
    column_names = [
        col.name if type(col) == Column else col.value.name for col in COLUMNS
    ]

    result_df = combined_df.withColumn(
        "signal_catalog_id",
        expr(SIGNAL_CATALOG_ID_HASH_EXPRESSION)
    ).select(*column_names)

    return result_df
