"""
MMYEF to Stream ID Snapshot Aggregate (Date-Partitioned)

Pre-computes distinct stream_ids per MMYEF from historical CAN traces up to each partition date.
This date-partitioned table enables efficient per-stream_id training candidate selection
while maintaining backfill consistency.

Key benefits:
- Date-partitioned for backfill consistency (each date sees only stream_ids discovered up to that date)
- Each partition contains cumulative snapshot of stream_ids up to that date
- Filters to proprietary broadcast signals (is_broadcast=TRUE, is_standard=FALSE)
- Includes ALL obd_values for a complete picture (downstream tables filter as needed)

Schema:
- date: Partition key
- mmyef_id: Vehicle characteristic hash
- stream_id: Unique CAN stream identifier
- obd_value: Signal type this stream_id maps to
- trace_count: Cumulative number of traces with this stream_id up to this date
- first_seen_date: When this stream_id was first discovered
- last_seen_date: Most recent trace with this stream_id up to this date

Each partition date contains only stream_ids that were first_seen_date <= partition_date,
ensuring backfill consistency - historical dates see only stream_ids that existed at that time.
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
-- Generate all dates in the partition range for per-date snapshots
dates_in_range AS (
    SELECT date_col AS partition_date
    FROM (
        SELECT explode(sequence(
            date('{date_start}'),
            date('{date_end}'),
            interval 1 day
        )) AS date_col
    )
),

-- For each partition date, aggregate stream_ids from daily table where date <= partition_date
-- This ensures backfill consistency - historical dates see only stream_ids that existed at that time
per_date_snapshots AS (
    SELECT
        d.partition_date AS date,
        amsd.mmyef_id,
        amsd.stream_id,
        amsd.obd_value,
        SUM(amsd.trace_count) AS trace_count,
        MIN(amsd.date) AS first_seen_date,
        MAX(amsd.date) AS last_seen_date
    FROM dates_in_range d
    CROSS JOIN {product_analytics_staging}.agg_mmyef_stream_ids_daily amsd
    WHERE amsd.date <= d.partition_date
    GROUP BY d.partition_date, amsd.mmyef_id, amsd.stream_id, amsd.obd_value
)

SELECT
    CAST(date AS STRING) AS date,
    mmyef_id,
    stream_id,
    obd_value,
    trace_count,
    first_seen_date,
    last_seen_date
FROM per_date_snapshots
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
            comment="Cumulative number of traces that contain this stream_id up to this date. "
            "Used for prioritization - lower counts may indicate underrepresented stream_ids."
        ),
    ),
    Column(
        name="first_seen_date",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Date when this stream_id was first discovered in a trace."
        ),
    ),
    Column(
        name="last_seen_date",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Most recent date when this stream_id was seen in a trace up to this partition date."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Date-partitioned snapshot aggregate of distinct stream_ids per MMYEF from historical CAN traces. "
        "Aggregates from agg_mmyef_stream_ids_daily to create per-date snapshots. Each partition date contains only "
        "stream_ids that were first_seen_date <= partition_date, ensuring backfill consistency. "
        "Filters to proprietary broadcast signals (is_broadcast=TRUE, is_standard=FALSE) and includes ALL obd_values. "
        "Enables efficient per-stream_id training candidate selection.",
        row_meaning="A unique (date, mmyef_id, stream_id, obd_value) combination with cumulative trace statistics up to this date",
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
        AnyUpstream(ProductAnalyticsStaging.AGG_MMYEF_STREAM_IDS_DAILY),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_MMYEF_STREAM_IDS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_mmyef_stream_ids(context: AssetExecutionContext) -> str:
    """
    Generate date-partitioned snapshot aggregate of stream_ids per MMYEF.

    This asset pre-computes the distinct stream_ids discovered in historical
    CAN traces per MMYEF up to each partition date, enabling efficient per-stream_id
    training candidate selection.

    Key features:
    - Date-partitioned for backfill consistency (historical dates see only historical stream_ids)
    - Filters to proprietary broadcast signals (worth reverse engineering)
    - Includes ALL obd_values for a complete picture
    - Tracks cumulative trace_count for prioritization (underrepresented stream_ids)
    - Each partition date contains stream_ids with first_seen_date <= partition_date

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    return format_date_partition_query(QUERY, context)

