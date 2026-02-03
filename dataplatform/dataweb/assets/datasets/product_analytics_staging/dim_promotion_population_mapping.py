"""
Mapping table from signal promotions to signal catalog

Maps each promotion to:
- mmyef_id: for joining to coverage tables
- signal_catalog_id: for joining to dim_signal_catalog_definitions

This enables:
- Joining promotions to coverage data via mmyef_id
- Joining promotions to signal definitions via signal_catalog_id
- Simple dashboard queries without complex multi-column joins

signal_catalog_id is NULL when the promotion doesn't match any catalog entry
(e.g., new signals not yet in catalog).

Vehicle attributes via: signalpromotiondb.populations USING (population_uuid)
Signal definitions via: dim_signal_catalog_definitions USING (signal_catalog_id)
"""

from dataclasses import replace

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, SignalPromotionDb
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.query import format_query

from .dim_combined_signal_definitions import SIGNAL_CATALOG_ID_COLUMN_BASE
from .dim_device_vehicle_properties import MMYEF_HASH_EXPRESSION, MMYEF_ID
from .fct_signal_promotion_transitions import (
    PROMOTION_UUID_COLUMN,
    POPULATION_UUID_COLUMN,
)


COLUMNS = [
    # Key columns
    replace(PROMOTION_UUID_COLUMN, nullable=False, primary_key=True),
    ColumnType.SIGNAL_UUID,
    replace(POPULATION_UUID_COLUMN, nullable=False),
    replace(MMYEF_ID, nullable=True),
    # Join key to catalog - enables simple JOIN to dim_signal_catalog_definitions
    replace(
        SIGNAL_CATALOG_ID_COLUMN_BASE,
        nullable=True,
        metadata=Metadata(
            comment="FK to dim_signal_catalog_definitions. NULL if no catalog match found."
        ),
    ),
    # Promotion state info
    # Note: Vehicle attributes via JOIN to signalpromotiondb.populations
    # Note: Signal definition via JOIN to dim_signal_catalog_definitions using signal_catalog_id
    Column(
        name="stage",
        type=DataType.SHORT,
        nullable=True,
        metadata=Metadata(
            comment="Current promotion stage (0=INITIAL, 1=FAILED, 2=PRE_ALPHA, 3=ALPHA, 4=BETA, 5=GA)"
        ),
    ),
    Column(
        name="status",
        type=DataType.SHORT,
        nullable=True,
        metadata=Metadata(
            comment="Current promotion status (0=PENDING, 1=LIVE, 2=FAILED, 3=DEPRECATED)"
        ),
    ),
    Column(
        name="population_type",
        type=DataType.SHORT,
        nullable=True,
        metadata=Metadata(comment="Population type from SPS (e.g., MMYETPF=1)"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


QUERY = """
-- Normalize population vehicle attributes before hashing to ensure consistent
-- mmyef_id computation matching dim_combined_signal_definitions.py pattern
WITH normalized_populations AS (
    SELECT
        population_uuid,
        UPPER(make) AS make,
        UPPER(model) AS model,
        year,
        UPPER(engine_model) AS engine_model,
        powertrain,
        fuel_group,
        UPPER(trim) AS trim,
        type
    FROM signalpromotiondb.populations
),

-- Compute mmyef_id for each promotion
promotions_with_mmyef AS (
    SELECT
        ls.promotion_uuid,
        ls.signal_uuid,
        ls.population_uuid,
        {mmyef_hash_expression} AS mmyef_id,
        ls.current.stage AS stage,
        ls.current.status AS status,
        pop.type AS population_type
    FROM product_analytics_staging.fct_signal_promotion_latest_state ls
    JOIN normalized_populations pop
        ON ls.population_uuid = pop.population_uuid
)

SELECT
    p.promotion_uuid,
    p.signal_uuid,
    p.population_uuid,
    p.mmyef_id,
    
    -- Join to catalog to get signal_catalog_id (NULL if no match)
    cat.signal_catalog_id,
    
    -- Promotion state
    p.stage,
    p.status,
    p.population_type

FROM promotions_with_mmyef p
JOIN signalpromotiondb.signals sig
    ON p.signal_uuid = sig.signal_uuid
-- Match to catalog using mmyef_id + signal definition columns (null-safe equality)
LEFT JOIN product_analytics_staging.dim_signal_catalog_definitions cat
    ON p.mmyef_id <=> cat.mmyef_id
    AND sig.obd_value <=> cat.obd_value
    AND sig.data_identifier <=> cat.data_identifier
    AND CAST(sig.bit_start AS INTEGER) <=> cat.bit_start
    AND CAST(sig.bit_length AS INTEGER) <=> cat.bit_length
    AND sig.response_id <=> cat.response_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Mapping from signal promotions to signal_catalog_id and mmyef_id. Enables simple JOINs to dim_signal_catalog_definitions (via signal_catalog_id) and coverage tables (via mmyef_id).",
        row_meaning="Each row maps a promotion to catalog and coverage join keys. signal_catalog_id is NULL if no catalog match.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_SIGNAL_PROMOTION_LATEST_STATE),
        AnyUpstream(ProductAnalyticsStaging.DIM_SIGNAL_CATALOG_DEFINITIONS),
        AnyUpstream(SignalPromotionDb.POPULATIONS),
        AnyUpstream(SignalPromotionDb.SIGNALS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_PROMOTION_POPULATION_MAPPING.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_promotion_population_mapping(context: AssetExecutionContext) -> str:
    """
    Generate mapping from signal promotions to signal catalog and coverage keys.

    Provides two join keys:
    - mmyef_id: Join to coverage tables (agg_can_trace_collection_coverage, etc.)
    - signal_catalog_id: Join to dim_signal_catalog_definitions

    Example dashboard query:
        SELECT p.*, cat.obd_value, cat.description
        FROM dim_promotion_population_mapping p
        JOIN dim_signal_catalog_definitions cat USING (signal_catalog_id)
        WHERE p.stage = 5 AND p.status = 1  -- GA and LIVE

    For vehicle attributes:
        JOIN signalpromotiondb.populations USING (population_uuid)

    Note: signal_catalog_id is NULL when promotion doesn't match any catalog entry.
    """

    return format_query(
        query=QUERY,
        mmyef_hash_expression=MMYEF_HASH_EXPRESSION,
    )
