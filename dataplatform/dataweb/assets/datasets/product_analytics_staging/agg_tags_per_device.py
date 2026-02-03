"""
Aggregated Tags Per Device

Tracks collected trace counts per tag per device.
This table provides device-level aggregation similar to agg_tags_per_mmyef but at device granularity.

Key Features:
- Queries DynamoDB sensor_replay_traces for existing tag records
- Counts collected traces per (tag_name, org_id, device_id) combination
- Date-partitioned for backfill consistency
- Supports device-level diversity tracking in candidate selection queries

Use Cases:
- Per-device quota enforcement in candidate selection queries
- Device diversity tracking (avoid repeatedly selecting from same devices)
- Understanding collection coverage at device level
- Enables agg_tags_per_mmyef to aggregate from device-level (single source of truth)
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
    Dynamodb,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description


# Reusable Column Definitions
TAG_COUNTS_MAP_COLUMN_BASE = Column(
    name="tag_counts_map",
    type=map_of(DataType.STRING, DataType.LONG, value_contains_null=True),
    nullable=True,
    metadata=Metadata(
        comment="Map of tag_name -> collected_trace_count. "
        "Tags are collection criteria names (e.g., 'can-set-main-0', 'can-set-signals-0', 'can-set-representative-0', 'can-set-training-0', 'can-set-inference-0'). "
        "Tag-specific counts are accessible via map lookup (e.g., tag_counts_map['can-set-training-0'])."
    ),
)

TOTAL_COLLECTED_COUNT_COLUMN_BASE = Column(
    name="total_collected_count",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(
        comment="Total cumulative number of distinct traces collected up to this date (NOT a sum of per-tag counts). "
        "Counts each trace once even if it has multiple tags. A trace tagged with both 'can-set-training-0' and "
        "'can-set-inference-0' contributes 1 to total_collected_count, not 2. Counts all traces with source_date <= partition date. "
        "Used for diversity tracking in candidate selection queries (prioritizing entities with fewer collected traces). "
        "Enables backfill consistency by providing historical counts that existed at each point in time."
    ),
)


QUERY = """
WITH
-- Generate all dates in the partition range for per-date cumulative counts
dates_in_range AS (
    SELECT date_col as partition_date
    FROM (
        SELECT explode(sequence(
            date('{date_start}'), 
            date('{date_end}'), 
            interval 1 day
        )) as date_col
    )
),

-- Get all CAN library records (available traces)
sensor_replay_can_library_records AS (
    SELECT
        srt.TraceUuid
    FROM dynamodb.sensor_replay_traces srt
    WHERE srt.ItemTypeAndIdentifiers = 'collection#library#CAN'
),

-- Get all tag records with their trace associations
sensor_replay_tag_records AS (
    SELECT
        srt.TraceUuid,
        -- Extract tag name from ItemTypeAndIdentifiers (e.g., 'collection#tag#can-set-main-0' -> 'can-set-main-0')
        REGEXP_REPLACE(srt.ItemTypeAndIdentifiers, '^collection#tag#', '') AS tag_name
    FROM dynamodb.sensor_replay_traces srt
    WHERE srt.ItemTypeAndIdentifiers LIKE 'collection#tag#can-set-%'
),

-- Get trace info with source details
can_library_records AS (
    SELECT
        DATE_FORMAT(FROM_UNIXTIME(CAST(srt.SourceAssetMs AS BIGINT) / 1000), 'yyyy-MM-dd') AS source_date,
        CAST(srt.SourceOrgId AS BIGINT) AS org_id,
        CAST(srt.SourceDeviceId AS BIGINT) AS device_id,
        CAST(srt.SourceAssetMs AS BIGINT) AS start_time,
        srt.TraceUuid,
        -- Remove duplicates based on (org, deviceId, SourceAssetMs)
        ROW_NUMBER() OVER (
            PARTITION BY srt.SourceOrgId, srt.SourceDeviceId, srt.SourceAssetMs
            ORDER BY srt.TraceUuid
        ) as row_num
    FROM dynamodb.sensor_replay_traces srt
    WHERE srt.ItemTypeAndIdentifiers = 'info'
      AND srt.TraceUuid IN (
          SELECT TraceUuid 
          FROM sensor_replay_can_library_records
      )
),

-- Deduplicated library records
unique_library_records AS (
    SELECT
        source_date,
        org_id,
        device_id,
        start_time,
        TraceUuid
    FROM can_library_records
    WHERE row_num = 1
),

-- Join with tag records to get which tags apply to which traces
library_records_with_tags AS (
    SELECT
        ulr.source_date,
        ulr.org_id,
        ulr.device_id,
        ulr.start_time,
        ulr.TraceUuid,
        str.tag_name
    FROM unique_library_records ulr
    JOIN sensor_replay_tag_records str USING (TraceUuid)
),

-- Compute cumulative counts per date by cross joining dates with library records
-- For each partition date, count all traces with source_date <= partition_date
-- Group by device (org_id, device_id) and tag_name
cumulative_counts_per_date AS (
    SELECT
        d.partition_date,
        lrt.org_id,
        lrt.device_id,
        lrt.tag_name,
        COUNT(DISTINCT lrt.TraceUuid) AS collected_trace_count
    FROM dates_in_range d
    CROSS JOIN library_records_with_tags lrt
    WHERE lrt.source_date <= d.partition_date
    GROUP BY d.partition_date, lrt.org_id, lrt.device_id, lrt.tag_name
),

-- Compute total distinct trace count per device (regardless of tags)
-- This counts each trace once even if it has multiple tags
-- Cross join dates with unique library records (without tags) to get distinct traces per date
total_trace_counts_per_date AS (
    SELECT
        d.partition_date,
        ulr.org_id,
        ulr.device_id,
        COUNT(DISTINCT ulr.TraceUuid) AS total_collected_count
    FROM dates_in_range d
    CROSS JOIN unique_library_records ulr
    WHERE ulr.source_date <= d.partition_date
    GROUP BY d.partition_date, ulr.org_id, ulr.device_id
),

-- Aggregate tag counts into a map per device and join with total count
-- total_collected_count is computed separately (distinct trace count, not sum of tag counts)
-- MAX selects the single total_collected_count value (same for all rows per device)
device_tag_aggregates AS (
    SELECT
        ccpd.partition_date,
        ccpd.org_id,
        ccpd.device_id,
        MAP_FROM_ARRAYS(
            COLLECT_LIST(ccpd.tag_name),
            COLLECT_LIST(ccpd.collected_trace_count)
        ) AS tag_counts_map,
        MAX(ttcpd.total_collected_count) AS total_collected_count
    FROM cumulative_counts_per_date ccpd
    JOIN total_trace_counts_per_date ttcpd
        ON ccpd.partition_date = ttcpd.partition_date
        AND ccpd.org_id = ttcpd.org_id
        AND ccpd.device_id = ttcpd.device_id
    GROUP BY ccpd.partition_date, ccpd.org_id, ccpd.device_id
)

SELECT
    CAST(partition_date AS STRING) AS date,
    org_id,
    device_id,
    tag_counts_map,
    total_collected_count
FROM device_tag_aggregates
ORDER BY date, org_id, device_id
"""


COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    replace(TAG_COUNTS_MAP_COLUMN_BASE, primary_key=False),
    replace(TOTAL_COLLECTED_COUNT_COLUMN_BASE, primary_key=False),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Date-partitioned aggregated trace counts per device. "
        "Queries DynamoDB sensor_replay_traces to count collected traces per device, storing tag counts in a MAP<STRING, LONG> "
        "and providing total_collected_count for diversity tracking. Each partition date contains cumulative counts of all traces "
        "with source_date <= partition date, ensuring backfill consistency (historical dates see only historical counts). "
        "Supports device-level diversity tracking in candidate selection queries by providing collection counts that existed at each point in time. "
        "Tags are extracted from ItemTypeAndIdentifiers pattern 'collection#tag#<tag_name>'. "
        "This table enables agg_tags_per_mmyef to aggregate from device-level for a single source of truth.",
        row_meaning="Cumulative trace counts (total and per-tag) for a specific device up to this date, with tag counts stored in a MAP",
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
        AnyUpstream(Dynamodb.SENSOR_REPLAY_TRACES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TAGS_PER_DEVICE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_tags_per_device(context: AssetExecutionContext) -> str:
    """
    Aggregate collected trace counts per tag per device with date partitioning.

    This asset provides device-level aggregation of collected trace counts,
    enabling device-level diversity tracking and serving as the source of truth
    for device-level tag counts.

    Each partition date contains cumulative counts (all traces with source_date <= partition date),
    ensuring backfill consistency. Historical dates see only historical counts, not future data.

    Provides:
    - Date-partitioned collection counts per device (one row per device)
    - Tag counts stored in MAP<STRING, LONG> for flexible access
    - Total collected count for device diversity tracking (simplifies common case)
    - Cumulative counts up to each partition date for backfill consistency
    - Support for device-level diversity tracking in candidate selection queries
    - Single source of truth for device-level collection progress at each point in time
    - Enables agg_tags_per_mmyef to aggregate from device-level

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    return format_date_partition_query(QUERY, context)

