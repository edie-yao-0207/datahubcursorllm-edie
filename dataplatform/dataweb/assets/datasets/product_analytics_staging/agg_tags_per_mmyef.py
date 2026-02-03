"""
Aggregated Tags Per MMYEF

Date-partitioned aggregate table tracking collected trace counts per tag per MMYEF population.
Aggregates device-level tag counts from agg_tags_per_device to MMYEF level for quota
enforcement in CAN trace candidate selection queries.

Key Features:
- Aggregates from agg_tags_per_device (device-level tag counts stored in MAP)
- Joins with dim_device_vehicle_properties to get MMYEF
- Counts collected traces per (tag_name, mmyef_id) combination by summing device-level counts
- Date-partitioned with cumulative counts for backfill consistency
- Stores tag counts in MAP<STRING, LONG> for flexible access
- Supports quota enforcement in downstream candidate selection tables

Use Cases:
- Per-MMYEF quota enforcement in candidate selection queries (representative, training, inference)
- Understanding collection coverage across vehicle populations
- Tracking progress toward collection goals per MMYEF
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    AWSRegion,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    DQCheckMode,
    TableType,
    WarehouseWriteMode,
)
from dataclasses import replace

from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
    map_of,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

from .dim_device_vehicle_properties import MMYEF_ID
from .agg_tags_per_device import TAG_COUNTS_MAP_COLUMN_BASE, TOTAL_COLLECTED_COUNT_COLUMN_BASE


QUERY = """
WITH
-- Explode the tag_counts_map from agg_tags_per_device to get tag_name and collected_trace_count
device_tags_exploded AS (
    SELECT
        atpd.date,
        atpd.org_id,
        atpd.device_id,
        exploded.tag_name,
        exploded.collected_trace_count
    FROM {product_analytics_staging}.agg_tags_per_device atpd
    LATERAL VIEW explode(atpd.tag_counts_map) exploded AS tag_name, collected_trace_count
    WHERE atpd.date BETWEEN '{date_start}' AND '{date_end}'
      AND atpd.tag_counts_map IS NOT NULL
),

-- Join device-level tag counts with vehicle properties to get MMYEF
device_tags_with_mmyef AS (
    SELECT
        dte.date,
        dte.tag_name,
        dte.org_id,
        dte.device_id,
        dte.collected_trace_count,
        dvp.mmyef_id
    FROM device_tags_exploded dte
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON dte.date = dvp.date
        AND dte.org_id = dvp.org_id
        AND dte.device_id = dvp.device_id
    WHERE dvp.date BETWEEN '{date_start}' AND '{date_end}'
      AND dvp.mmyef_id IS NOT NULL
),

-- Aggregate device-level tag counts to MMYEF level per tag
-- SUM preserves cumulative aggregation for backfill consistency
mmyef_tag_counts AS (
    SELECT
        date,
        tag_name,
        mmyef_id,
        SUM(collected_trace_count) AS collected_trace_count
    FROM device_tags_with_mmyef
    GROUP BY date, tag_name, mmyef_id
),

-- Get device-level total_collected_count (distinct trace count per device)
device_total_counts AS (
    SELECT DISTINCT
        atpd.date,
        atpd.org_id,
        atpd.device_id,
        atpd.total_collected_count
    FROM {product_analytics_staging}.agg_tags_per_device atpd
    WHERE atpd.date BETWEEN '{date_start}' AND '{date_end}'
      AND atpd.total_collected_count IS NOT NULL
),

-- Join device-level totals with vehicle properties to get MMYEF
device_totals_with_mmyef AS (
    SELECT
        dtc.date,
        dtc.org_id,
        dtc.device_id,
        dtc.total_collected_count,
        dvp.mmyef_id
    FROM device_total_counts dtc
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        ON dtc.date = dvp.date
        AND dtc.org_id = dvp.org_id
        AND dtc.device_id = dvp.device_id
    WHERE dvp.date BETWEEN '{date_start}' AND '{date_end}'
      AND dvp.mmyef_id IS NOT NULL
),

-- Aggregate device-level totals to MMYEF level
-- Uses total_collected_count (not per-tag counts) to avoid double-counting traces with multiple tags
mmyef_total_counts AS (
    SELECT
        date,
        mmyef_id,
        SUM(total_collected_count) AS total_collected_count
    FROM device_totals_with_mmyef
    GROUP BY date, mmyef_id
),

-- Aggregate tags into map and join with total count
mmyef_tag_aggregates AS (
    SELECT
        mtc.date,
        mtc.mmyef_id,
        MAP_FROM_ARRAYS(
            COLLECT_LIST(mtc.tag_name),
            COLLECT_LIST(mtc.collected_trace_count)
        ) AS tag_counts_map,
        MAX(mttc.total_collected_count) AS total_collected_count
    FROM mmyef_tag_counts mtc
    JOIN mmyef_total_counts mttc
        ON mtc.date = mttc.date
        AND mtc.mmyef_id = mttc.mmyef_id
    GROUP BY mtc.date, mtc.mmyef_id
)

SELECT
    CAST(date AS STRING) AS date,
    mmyef_id,
    tag_counts_map,
    total_collected_count
FROM mmyef_tag_aggregates
ORDER BY date, mmyef_id
"""


COLUMNS = [
    ColumnType.DATE,
    replace(MMYEF_ID, primary_key=True, nullable=False),
    replace(TAG_COUNTS_MAP_COLUMN_BASE, primary_key=False),
    replace(TOTAL_COLLECTED_COUNT_COLUMN_BASE, primary_key=False),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Date-partitioned aggregated trace counts per MMYEF population. "
        "Aggregates from agg_tags_per_device (device-level tag counts stored in MAP) by exploding the tag_counts_map, "
        "joining with dim_device_vehicle_properties to get MMYEF, and aggregating device-level counts. "
        "Stores tag counts in a MAP<STRING, LONG> and provides total_collected_count for diversity tracking. "
        "Each partition date contains cumulative counts ensuring backfill consistency (historical dates see only historical counts). "
        "Supports quota enforcement in candidate selection queries by providing collection counts that existed at each point in time.",
        row_meaning="Cumulative trace counts (total and per-tag) for a specific MMYEF up to this date, with tag counts stored in a MAP",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_TAGS_PER_DEVICE),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TAGS_PER_MMYEF.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_tags_per_mmyef(context: AssetExecutionContext) -> str:
    """
    Aggregate collected trace counts per MMYEF with date partitioning.

    This asset aggregates device-level tag counts from agg_tags_per_device to MMYEF level,
    providing per-population collection statistics for quota enforcement in downstream
    candidate selection queries.

    Aggregates from agg_tags_per_device (device-level tag counts stored in MAP) by exploding
    the tag_counts_map, joining with dim_device_vehicle_properties to get MMYEF, and
    aggregating device-level counts. Cumulative counts are preserved at MMYEF level,
    ensuring backfill consistency (historical dates see only historical counts).

    Provides:
    - Date-partitioned collection counts per MMYEF (one row per MMYEF per date)
    - Tag counts stored in MAP<STRING, LONG> for flexible access
    - Total collected count for MMYEF diversity tracking
    - Cumulative counts up to each partition date for backfill consistency
    - Support for quota enforcement in candidate selection queries (fct_can_trace_representative_candidates,
      fct_can_trace_training_by_stream_id, fct_can_trace_reverse_engineering_candidates, etc.)

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    return format_date_partition_query(QUERY, context)

