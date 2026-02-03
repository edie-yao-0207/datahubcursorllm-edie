"""
Trace Characteristics Dimension Asset

This asset provides comprehensive summary statistics about CAN trace characteristics
to enable proactive performance tuning and skew management. It eliminates the need
for expensive runtime skew analysis by pre-computing trace-level metrics.

Key benefits:
- Proactive skew detection and capacity planning
- Fast lookup for processing decisions  
- Historical workload pattern analysis
- Performance bottleneck identification
"""

from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
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
    map_of,
    struct_of,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description


COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.TRACE_UUID,
    Column(
        "interface_can_id_stats",
        map_of(
            DataType.STRING,
            map_of(
                DataType.LONG,
                struct_of(
                    ("frame_count", DataType.LONG, True),
                    ("min_payload_length", DataType.INTEGER, True),
                    ("max_payload_length", DataType.INTEGER, True),
                    ("avg_payload_length", DataType.DOUBLE, True),
                    ("first_seen_unix_us", DataType.LONG, True),
                    ("last_seen_unix_us", DataType.LONG, True),
                ),
                value_contains_null=True,
            ),
            value_contains_null=True,
        ),
        metadata=Metadata(
            comment="Nested map: interface -> can_id -> stats for each data stream combination"
        ),
    ),
    Column(
        "total_frames",
        DataType.LONG,
        metadata=Metadata(comment="Total number of CAN frames in the trace"),
    ),
    Column(
        "unique_interfaces",
        DataType.LONG,
        metadata=Metadata(comment="Number of unique source interfaces"),
    ),
    Column(
        "unique_can_ids",
        DataType.LONG,
        metadata=Metadata(
            comment="Number of unique CAN arbitration IDs across all interfaces"
        ),
    ),
    Column(
        "trace_start_unix_us",
        DataType.LONG,
        metadata=Metadata(
            comment="Trace start timestamp in microseconds since Unix epoch"
        ),
    ),
    Column(
        "trace_end_unix_us",
        DataType.LONG,
        metadata=Metadata(
            comment="Trace end timestamp in microseconds since Unix epoch"
        ),
    ),
    Column(
        "trace_duration_seconds",
        DataType.DOUBLE,
        metadata=Metadata(comment="Total trace duration in seconds"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
WITH

-- Get stats per (interface, can_id) combination
interface_can_id_metrics AS (
    SELECT 
        date, org_id, device_id, trace_uuid, source_interface, id,
        COUNT(*) as frame_count,
        MIN(LENGTH(payload)) as min_payload_length,
        MAX(LENGTH(payload)) as max_payload_length,
        AVG(LENGTH(payload)) as avg_payload_length,
        MIN(timestamp_unix_us) as first_seen_unix_us,
        MAX(timestamp_unix_us) as last_seen_unix_us
    FROM product_analytics_staging.fct_sampled_can_frames
    WHERE date BETWEEN '{date_start}' AND '{date_end}'
    GROUP BY date, org_id, device_id, trace_uuid, source_interface, id
),

-- Build nested map per interface: can_id -> stats
interface_maps AS (
    SELECT
        date, org_id, device_id, trace_uuid, source_interface,
        MAP_FROM_ARRAYS(
            COLLECT_LIST(CAST(id AS BIGINT)),
            COLLECT_LIST(NAMED_STRUCT(
                'frame_count', frame_count,
                'min_payload_length', min_payload_length,
                'max_payload_length', max_payload_length,
                'avg_payload_length', avg_payload_length,
                'first_seen_unix_us', first_seen_unix_us,
                'last_seen_unix_us', last_seen_unix_us
            ))
        ) as can_id_stats_map
    FROM interface_can_id_metrics
    GROUP BY date, org_id, device_id, trace_uuid, source_interface
),

-- Get trace-level temporal metrics
trace_temporal AS (
    SELECT
        date, org_id, device_id, trace_uuid,
        MIN(timestamp_unix_us) as trace_start_unix_us,
        MAX(timestamp_unix_us) as trace_end_unix_us,
        CAST((MAX(timestamp_unix_us) - MIN(timestamp_unix_us)) / 1000000.0 AS DOUBLE) as trace_duration_seconds
    FROM product_analytics_staging.fct_sampled_can_frames
    WHERE date BETWEEN '{date_start}' AND '{date_end}'
    GROUP BY date, org_id, device_id, trace_uuid
)

-- Final result: nested map structure + trace-level aggregates
SELECT
    im.date,
    im.org_id, 
    im.device_id,
    im.trace_uuid,
    
    -- Nested map: interface -> can_id -> stats
    MAP_FROM_ARRAYS(
        COLLECT_LIST(im.source_interface),
        COLLECT_LIST(im.can_id_stats_map)
    ) as interface_can_id_stats,
    
    -- Trace-level summary metrics
    SUM(AGGREGATE(MAP_VALUES(im.can_id_stats_map), CAST(0 AS BIGINT), (acc, stats) -> acc + stats.frame_count)) as total_frames,
    COUNT(DISTINCT im.source_interface) as unique_interfaces,
    SUM(CARDINALITY(im.can_id_stats_map)) as unique_can_ids,
    
    -- Temporal characteristics from separate CTE
    tt.trace_start_unix_us,
    tt.trace_end_unix_us,
    tt.trace_duration_seconds

FROM interface_maps im
LEFT JOIN trace_temporal tt USING (date, org_id, device_id, trace_uuid)
GROUP BY im.date, im.org_id, im.device_id, im.trace_uuid, tt.trace_start_unix_us, tt.trace_end_unix_us, tt.trace_duration_seconds
ORDER BY total_frames DESC
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Nested data stream statistics organized by interface and CAN ID",
        row_meaning="Provides hierarchical statistics: interface -> CAN ID -> detailed metrics (frame counts, payload characteristics, temporal info) for each trace.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_SAMPLED_CAN_FRAMES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_TRACE_CHARACTERISTICS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_trace_characteristics(context: AssetExecutionContext) -> str:
    """
    Generate comprehensive trace characteristics for performance analysis and skew management.

    This asset analyzes CAN trace patterns to provide:
    - Frame distribution across source interfaces (interface-level skew detection)
    - Frame distribution across CAN IDs (message-level analysis for heavy/light streams)
    - Transport protocol complexity (UDF processing requirements)
    - Temporal patterns (trace durations and timing)
    - Processing complexity scores for capacity planning

    Key use cases:
    1. Proactive skew detection before processing
    2. Capacity planning and cluster sizing
    3. Performance bottleneck identification
    4. Historical workload pattern analysis
    """
    return format_date_partition_query(QUERY, context)
