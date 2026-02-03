"""
Signal Catalog Definitions Asset

This asset creates a comprehensive signal catalog by combining existing signal definitions
from dim_combined_signal_definitions with optional sources when available:
- AI/ML reverse engineering predictions from dojo.signal_reverse_engineering_predictions_v2
- Telematics promotion gaps from fct_telematics_promotion_gaps

The asset uses a modular query pattern to conditionally merge signals from multiple sources
based on table availability in the current region.
"""

from dataclasses import dataclass, replace
from typing import Any, Dict, List

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    ALL_COMPUTE_REGIONS,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.schema import (
    ColumnType,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import Definitions, Dojo, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_query
from dataweb.userpkgs.utils import build_table_description

from .dim_combined_signal_definitions import (
    SIGNAL_CATALOG_ID_COLUMN_BASE,
    SIGNAL_CATALOG_ID_HASH_EXPRESSION,
    SignalSourceType,
)
from .dim_device_vehicle_properties import MMYEF_ID

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
    ColumnType.STREAM_ID,
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@dataclass
class SignalSource:
    """Configuration for an optional signal source to merge into the catalog."""

    name: str
    table_name: str
    query_component: str
    required_upstreams: List[str]  # Additional tables needed for this source (e.g., lookup tables)
    format_args: Dict[str, Any]  # Arguments to pass to format_query for this component


# Base query: Always included - signals from dim_combined_signal_definitions
BASE_QUERY_COMPONENT = """
    SELECT
        data_identifier,
        make,
        model,
        year,
        engine_model,
        powertrain,
        fuel_group,
        trim,
        mmyef_id,
        obd_value,
        bit_start,
        bit_length,
        endian,
        scale,
        internal_scaling,
        sign,
        offset,
        minimum,
        maximum,
        unit,
        description,
        mapping,
        pgn,
        spn,
        passive_response_mask,
        response_id,
        request_id,
        source_id,
        application_id,
        data_source_id,
        protocol_id,
        signal_uuid,
        request_period_ms,
        is_broadcast,
        is_standard,
        stream_id
    FROM {product_analytics_staging}.dim_combined_signal_definitions
"""

# Optional source: AI/ML reverse engineering predictions v2
AI_PREDICTIONS_V2_COMPONENT = """
    SELECT
        CAST(
            CASE
                WHEN data_id_int = 0 THEN NULL
                WHEN ISNAN(data_id_int) THEN NULL
                ELSE data_id_int
            END AS BIGINT
        ) AS data_identifier,
        make,
        model,
        year,
        engine_model,
        CAST(powertrain AS INTEGER) AS powertrain,
        CAST(fuel_group AS INTEGER) AS fuel_group,
        trim,
        mmyef_id,

        CAST(obd_values.id AS BIGINT) AS obd_value,
        CAST(bit_start AS INTEGER) AS bit_start,
        CAST(bit_length AS INTEGER) AS bit_length,
        endian,
        COALESCE(scale, 1.0) AS scale,
        COALESCE(internal_scaling, 1.0) AS internal_scaling,
        CAST(COALESCE(sign, 0) AS BIGINT) AS sign, -- 0 is SIGN_INVALID
        COALESCE(offset, 0.0) AS offset,
        DOUBLE('NaN') AS minimum,
        DOUBLE('NaN') AS maximum,
        CAST(NULL AS STRING) AS unit,

        FORMAT_STRING(
            "AI/ML Generated Rule | Confidence %.1f%% | Scale-offset-sign confidence %.1f%% | %s traces",
            prediction_confidence * 100,
            COALESCE(scale_offset_sign_confidence * 100, -1),
            number_of_traces
        ) AS description,
        CAST(NULL AS STRING) AS mapping,
        CAST(NULL AS INTEGER) AS pgn,
        CAST(NULL AS INTEGER) AS spn,

        -- 0x1FFFFFFF (536870911)
        CAST(536870911 AS BIGINT) AS passive_response_mask,
        CAST(CONV(source_address_hex, 16, 10) AS BIGINT) AS response_id,
        CAST(NULL AS BIGINT) AS request_id,

        CAST(NULL AS INTEGER) AS source_id,
        CAST(0 AS INTEGER) AS application_id, -- NONE
        CAST({ai_source} AS INTEGER) AS data_source_id,  -- Mark as AI/ML source

        CAST(NULL AS BIGINT) AS protocol_id,
        CAST(NULL AS STRING) AS signal_uuid,

        CAST(NULL AS BIGINT) AS request_period_ms,
        -- We're currently only running inference on broadcast signals that are proprietary
        CAST(TRUE AS BOOLEAN) AS is_broadcast,
        CAST(FALSE AS BOOLEAN) AS is_standard,

        CAST(NULL AS BIGINT) AS stream_id

    FROM dojo.signal_reverse_engineering_predictions_v2
    LEFT JOIN definitions.obd_values ON name = signal
"""

# Optional source: Telematics promotion gaps (unreviewed gaps with device coverage)
TELEMATICS_GAPS_COMPONENT = """
    SELECT
        data_identifier,
        make,
        model,
        year,
        engine_model,
        powertrain,
        fuel_group,
        trim,
        mmyef_id,

        obd_value,
        bit_start,
        bit_length,
        endian,
        scale,
        internal_scaling,
        sign,
        offset,
        minimum,
        maximum,
        CAST(NULL AS STRING) AS unit,

        CASE 
            WHEN audit_notes IS NOT NULL THEN CONCAT('Promotion Gap | ', audit_notes)
            ELSE 'Promotion Gap | Derived from nearest year template'
        END AS description,
        CAST(NULL AS STRING) AS mapping,
        CAST(NULL AS INTEGER) AS pgn,
        CAST(NULL AS INTEGER) AS spn,

        -- Use standard passive response mask
        CAST(536870911 AS BIGINT) AS passive_response_mask,
        response_id,
        request_id,

        CAST(NULL AS INTEGER) AS source_id,
        CAST(0 AS INTEGER) AS application_id, -- NONE
        CAST({gap_source} AS INTEGER) AS data_source_id,  -- Mark as promotion gap source

        CAST(NULL AS BIGINT) AS protocol_id,
        CAST(NULL AS STRING) AS signal_uuid,

        CAST(NULL AS BIGINT) AS request_period_ms,
        is_broadcast,
        is_standard,

        CAST(NULL AS BIGINT) AS stream_id

    FROM {product_analytics_staging}.fct_telematics_promotion_gaps
"""

# Registry of optional signal sources
# Add new sources here to automatically support conditional merging
#
# To add a new source:
# 1. Define a new query component constant (like AI_PREDICTIONS_V2_COMPONENT)
# 2. Add a SignalSource entry to this list with:
#    - name: Human-readable name for logging
#    - table_name: Fully qualified table name (schema.table)
#    - query_component: The SQL query component to union
#    - required_upstreams: List of additional tables needed (for joins, lookups, etc.)
#    - format_args: Dict of parameters to pass to format_query (e.g., {"ai_source": SignalSourceType.AI})
# 3. The source will automatically be checked and included if available
OPTIONAL_SIGNAL_SOURCES = [
    SignalSource(
        name="AI/ML Predictions V2",
        table_name=str(Dojo.SIGNAL_REVERSE_ENGINEERING_PREDICTIONS_V2),
        query_component=AI_PREDICTIONS_V2_COMPONENT,
        required_upstreams=[str(Definitions.OBD_VALUES)],
        format_args={"ai_source": SignalSourceType.AI},
    ),
    SignalSource(
        name="Telematics Promotion Gaps",
        table_name=str(ProductAnalyticsStaging.FCT_TELEMATICS_PROMOTION_GAPS),
        query_component=TELEMATICS_GAPS_COMPONENT,
        required_upstreams=[],  # No additional tables needed beyond what gaps table already depends on
        format_args={"gap_source": SignalSourceType.PROMOTION_GAP},
    ),
]

# Template for the final query
QUERY_TEMPLATE = """
-- Comprehensive signal catalog combining multiple sources
WITH combined_signals AS (
{union_query}
)

SELECT
    -- Generate deterministic signal_catalog_id using xxhash64
    {signal_catalog_id_hash} AS signal_catalog_id,

    -- All other columns
    data_identifier,
    make,
    model,
    year,
    engine_model,
    powertrain,
    fuel_group,
    trim,
    mmyef_id,
    obd_value,
    bit_start,
    bit_length,
    endian,
    scale,
    internal_scaling,
    sign,
    offset,
    minimum,
    maximum,
    unit,
    description,
    mapping,
    pgn,
    spn,
    passive_response_mask,
    response_id,
    request_id,
    source_id,
    application_id,
    data_source_id,
    protocol_id,
    signal_uuid,
    request_period_ms,
    is_broadcast,
    is_standard,
    stream_id

FROM combined_signals
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Comprehensive signal catalog combining existing signal definitions with optional AI/ML predictions, promotion gaps, and other sources when available in the region",
        row_meaning="Each row represents a signal definition from any source (existing catalog, AI predictions, promotion gaps, etc.) with unified schema and source tracking via data_source_id.",
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_COMBINED_SIGNAL_DEFINITIONS),
        AnyUpstream(Definitions.OBD_VALUES),
        AnyUpstream(Dojo.SIGNAL_REVERSE_ENGINEERING_PREDICTIONS_V2),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_PROMOTION_GAPS),
        # Note: Optional sources are checked at runtime and conditionally merged if available.
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_SIGNAL_CATALOG_DEFINITIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def dim_signal_catalog_definitions(context: AssetExecutionContext) -> str:
    """
    Generate comprehensive signal catalog combining existing definitions with optional sources.

    This asset creates a unified signal catalog by combining:
    1. Existing dim_combined_signal_definitions (J1939, SPS, passenger signals) - always included
    2. AI/ML reverse engineering predictions from dojo.signal_reverse_engineering_predictions_v2 - if available
    3. Telematics promotion gaps from fct_telematics_promotion_gaps - if available
    4. Additional future sources - if available

    The query is built dynamically based on which optional tables exist in the current region.
    This allows the asset to run in regions where some source tables may not be available.

    Key features:
    - Modular query pattern for conditional source merging
    - Runtime table existence checks
    - Unified schema ensuring compatibility across all signal sources
    - Proper handling of missing columns from optional sources
    - AI predictions marked with SignalSourceType.AI (data_source_id=5)
    - Promotion gaps marked with SignalSourceType.PROMOTION_GAP (data_source_id=6) and descriptive text
    - Easy extensibility for future signal sources
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    def table_exists(table_name: str) -> bool:
        """Check if a table exists using SQL (works with Spark Connect)."""
        try:
            spark.sql(f"DESCRIBE TABLE {table_name}").collect()
            return True
        except Exception:
            return False

    # Format base query component (always included)
    formatted_components = [format_query(BASE_QUERY_COMPONENT)]

    # Add optional sources if available, formatting each one
    for source in OPTIONAL_SIGNAL_SOURCES:
        try:
            if table_exists(source.table_name) and all(
                table_exists(u) for u in source.required_upstreams
            ):
                # Format the source query component with its specific parameters
                formatted_component = format_query(
                    source.query_component,
                    **source.format_args
                )
                formatted_components.append(formatted_component)
                context.log.info(f"Including optional source: {source.name}")
            else:
                context.log.info(
                    f"Skipping optional source: {source.name} (table or upstreams not available)"
                )
        except Exception as e:
            context.log.warning(f"Error checking {source.name}: {e}. Skipping.")

    # Join all formatted components into union query
    union_query = "\n\n    UNION ALL\n".join(formatted_components)

    # Build and return the final query
    return QUERY_TEMPLATE.format(
        union_query=union_query,
        signal_catalog_id_hash=SIGNAL_CATALOG_ID_HASH_EXPRESSION,
    )
