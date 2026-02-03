"""
Daily Aggregate: MMYEF to Stream ID

Daily aggregation of distinct stream_ids per MMYEF from CAN traces.
This intermediate table aggregates daily data from fct_can_trace_recompiled,
reducing scan volume for the final snapshot aggregate.

Key benefits:
- Partitioned by date = incremental processing
- Processes one day at a time from fct_can_trace_recompiled
- Joins with dim_combined_signal_definitions to get obd_value
- Filters to proprietary broadcast signals (is_broadcast=TRUE, is_standard=FALSE)
- Feeds into agg_mmyef_stream_ids for final snapshot aggregation

Schema:
- date: Partition key
- mmyef_id: Vehicle characteristic hash
- stream_id: Unique CAN stream identifier
- obd_value: Signal type this stream_id maps to
- trace_count: Number of distinct traces with this stream_id on this day
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

from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

from .dim_device_vehicle_properties import MMYEF_ID

QUERY = """
WITH
-- Get distinct stream_ids from recompiled traces for this date partition
-- fct_can_trace_recompiled already computes stream_id per frame
traces_with_stream_ids AS (
    SELECT DISTINCT
        fctr.date,
        fctr.org_id,
        fctr.device_id,
        fctr.trace_uuid,
        fctr.mmyef_id,
        fctr.stream_id
    FROM product_analytics_staging.fct_can_trace_recompiled fctr
    WHERE fctr.date BETWEEN '{date_start}' AND '{date_end}'
      AND fctr.mmyef_id IS NOT NULL
      AND fctr.stream_id IS NOT NULL
),

-- Join with signal definitions to get obd_value and filter to proprietary broadcast signals
-- Join on mmyef_id + stream_id to get the correct signal definition
trace_stream_ids_with_obd AS (
    SELECT DISTINCT
        tws.date,
        tws.org_id,
        tws.device_id,
        tws.trace_uuid,
        tws.mmyef_id,
        tws.stream_id,
        dcsd.obd_value
    FROM traces_with_stream_ids tws
    JOIN product_analytics_staging.dim_combined_signal_definitions dcsd
        ON tws.mmyef_id = dcsd.mmyef_id
        AND tws.stream_id = dcsd.stream_id
    WHERE dcsd.is_broadcast = TRUE
      AND dcsd.is_standard = FALSE
      AND dcsd.obd_value IS NOT NULL
),

-- Aggregate per day: distinct (mmyef_id, stream_id, obd_value) with trace counts
aggregated AS (
    SELECT
        date,
        mmyef_id,
        stream_id,
        obd_value,
        COUNT(DISTINCT org_id, device_id, trace_uuid) AS trace_count
    FROM trace_stream_ids_with_obd
    GROUP BY date, mmyef_id, stream_id, obd_value
)

SELECT
    date,
    mmyef_id,
    stream_id,
    obd_value,
    trace_count
FROM aggregated
ORDER BY date, mmyef_id, obd_value, stream_id
"""

COLUMNS = [
    ColumnType.DATE,
    replace(MMYEF_ID, primary_key=True, nullable=False),
    replace(ColumnType.STREAM_ID.value, nullable=False, primary_key=True),
    replace(ColumnType.OBD_VALUE.value, nullable=False, primary_key=True),
    Column(
        name="trace_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of distinct traces that contain this stream_id on this day. "
            "Used by agg_mmyef_stream_ids for final aggregation across all dates."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily aggregate of distinct stream_ids per MMYEF from CAN traces. "
        "Aggregates daily data from fct_can_trace_recompiled (which already computes stream_id per frame) "
        "and joins with dim_combined_signal_definitions to get obd_value. "
        "Filters to proprietary broadcast signals (is_broadcast=TRUE, is_standard=FALSE). "
        "This intermediate table reduces scan volume for agg_mmyef_stream_ids by pre-aggregating per day. "
        "Partitioned by date to enable incremental processing.",
        row_meaning="A unique (date, mmyef_id, stream_id, obd_value) combination with daily trace count",
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
        AnyUpstream(ProductAnalyticsStaging.FCT_CAN_TRACE_RECOMPILED),
        AnyUpstream(ProductAnalyticsStaging.DIM_COMBINED_SIGNAL_DEFINITIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_MMYEF_STREAM_IDS_DAILY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_mmyef_stream_ids_daily(context: AssetExecutionContext) -> str:
    """
    Generate daily aggregate of stream_ids per MMYEF.

    This asset pre-aggregates daily data from fct_can_trace_recompiled to reduce
    scan volume for the final snapshot aggregate (agg_mmyef_stream_ids).

    Key features:
    - Partitioned by date for incremental processing
    - Processes one day at a time from fct_can_trace_recompiled
    - Filters to proprietary broadcast signals
    - Joins with dim_combined_signal_definitions to get obd_value
    - Counts distinct traces per stream_id per day

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    return format_date_partition_query(QUERY, context)

