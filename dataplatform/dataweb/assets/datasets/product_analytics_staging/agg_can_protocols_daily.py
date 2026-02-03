"""
agg_can_protocols_daily

Daily aggregation of CAN protocol detection events per device with occurrence counts.
Processes raw protocol detection events from Kinesis and provides device-level daily summaries
of detected protocols with their names and detection frequencies.
"""

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
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
    struct_with_comments,
    array_of,
)
from dataweb.userpkgs.firmware.table import (
    KinesisStatsHistory,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="protocols",
        type=array_of(
            struct_with_comments(
                (
                    "protocol_id",
                    DataType.INTEGER,
                    "Raw protocol ID from the Kinesis event",
                ),
                (
                    "detection_count",
                    DataType.LONG,
                    "Number of times this protocol was detected on this device on this day",
                ),
                (
                    "times_selected",
                    DataType.LONG,
                    "Number of times this protocol was selected to run",
                ),
            )
        ),
        nullable=True,
        metadata=Metadata(
            comment="Array of detected CAN protocols with their IDs, detection counts, and selection frequencies. "
            "Each struct represents a unique protocol detected on this device on this day. "
            "Protocol names can be joined from definitions.obd_protocol when needed for visualization."
        ),
    ),
    Column(
        name="unique_protocols",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of unique protocols detected on this device on this day"
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
WITH protocol_events AS (
    SELECT
        date,
        org_id,
        object_id AS device_id,
        entry.protocol AS protocol_id,
        CASE WHEN entry.selected_to_run THEN 1 ELSE 0 END AS was_selected
    FROM kinesisstats_history.osdcanprotocolsdetected
    LATERAL VIEW EXPLODE(value.proto_value.can_protocols_detected.detected_can_protocols) AS entry
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
        AND NOT value.is_end
        AND NOT value.is_databreak
),

protocol_counts AS (
    SELECT 
        date,
        org_id,
        device_id,
        protocol_id,
        COUNT(*) AS detection_count,
        SUM(was_selected) AS times_selected
    FROM protocol_events
    GROUP BY date, org_id, device_id, protocol_id
),

protocol_aggregated AS (
    SELECT 
        date,
        org_id,
        device_id,
        COLLECT_LIST(STRUCT(
            protocol_id,
            detection_count,
            times_selected
        )) AS protocols,
        COUNT(DISTINCT protocol_id) AS unique_protocols
    FROM protocol_counts
    GROUP BY date, org_id, device_id
)

SELECT 
    date,
    org_id,
    device_id,
    protocols,
    unique_protocols
FROM protocol_aggregated
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily aggregation of CAN protocol detection events per device with occurrence counts and selection frequencies. "
        "Processes raw protocol detection events from osdcanprotocolsdetected Kinesis stream and provides device-level daily summaries "
        "of detected protocols with their IDs, detection counts per protocol, and how often they were selected to run. "
        "Includes count of unique protocols detected. Protocol names can be joined from definitions.obd_protocol at query time. "
        "Complements fct_dtc_events_daily by providing the CAN protocol context that can be joined on date/org_id/device_id.",
        row_meaning="Each row represents the complete CAN protocol detection activity for a device for one day, containing arrays of "
        "detected protocol IDs with their individual detection frequencies, selection counts, and a summary count of unique protocols.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStatsHistory.OSD_CAN_PROTOCOLS_DETECTED),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_CAN_PROTOCOLS_DAILY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_can_protocols_daily(context: AssetExecutionContext) -> str:
    """
    Aggregate CAN protocol detection events per device per day with counts and selection frequencies.

    This asset:
    1. Extracts protocol detection events from osdcanprotocolsdetected Kinesis stream
    2. Counts how many times each protocol was detected per device per day
    3. Tracks how many times each protocol was selected to run
    4. Aggregates into structured arrays with protocol IDs and summary statistics

    Protocol names can be joined from definitions.obd_protocol at query/visualization time.
    The result provides a clean daily view of CAN protocol activity per device, supporting
    protocol analysis and diagnostics research. Can be easily joined with fct_dtc_events_daily
    or other device-level daily tables on date/org_id/device_id.
    """
    return format_date_partition_query(QUERY, context)
