"""
Command Scheduler Statistics

This asset processes OBD command scheduler statistics from Kinesis streams, providing
normalized diagnostic data about request/response patterns and command derate behavior.
The data is collected into arrays per device to minimize row explosion while preserving
detailed command-level statistics for analysis.

Key Features:
- Flattens nested command scheduler statistics from protobuf data into structured arrays
- Collects all commands per device/date into a single row to reduce data volume
- Converts binary identifiers to hex format for readability
- Provides both detailed command arrays and summary statistics per device
- Preserves individual command metrics while enabling efficient storage and querying

Processing Logic:
1. Read command scheduler stats from Kinesis stream
2. Explode nested command_stats array using LATERAL VIEW
3. Convert binary IDs (request_id, response_id, data_identifier) to hex
4. Create structured command objects including bus_id with request/response and derate statistics
5. Collect all commands from all buses into arrays grouped by device and date only
6. Calculate summary statistics across all commands and buses per device

Use Cases:
- Diagnostic command performance analysis per device
- OBD request/response reliability monitoring across command sets
- Command scheduler health assessment with reduced data footprint
- Derate behavior analysis and optimization
- Efficient querying of device-level diagnostic patterns
"""

from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
    FRESHNESS_SLO_9AM_PST,
    FIRMWAREVDP,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
    array_of,
    struct_of,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, KinesisStats
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description


QUERY = """
WITH
  data AS (
    SELECT *
    FROM kinesisstats.osDCommandSchedulerStats
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
      AND value.proto_value IS NOT NULL
  ),
  
  exploded_commands AS (
    SELECT
      date,
      org_id,
      object_id AS device_id,
      STRUCT(
        value.proto_value.command_scheduler_stats.bus_id AS bus_id,
        hex(element.command.request_id) AS request_id,
        hex(element.command.response_id) AS response_id,
        hex(element.command.data_identifier) AS data_identifier,
        COALESCE(element.request_response_stat.request_count, 0) AS request_count,
        COALESCE(element.request_response_stat.any_response_count, 0) AS any_response_count,
        COALESCE(element.derated_stat.latest_derate_offset_ms, 0) AS derate_offset_ms,
        COALESCE(element.derated_stat.overall_derate_instances, 0) AS derate_instances
      ) AS command_stat
    FROM data
    LATERAL VIEW EXPLODE(value.proto_value.command_scheduler_stats.command_stats) AS element
  )

SELECT
  date,
  org_id,
  device_id,
  COLLECT_LIST(command_stat) AS command_stats,
  CAST(SIZE(COLLECT_LIST(command_stat)) AS BIGINT) AS total_commands,
  CAST(SUM(command_stat.request_count) AS BIGINT) AS total_request_count,
  CAST(SUM(command_stat.any_response_count) AS BIGINT) AS total_any_response_count,
  CAST(SUM(command_stat.derate_offset_ms) AS BIGINT) AS total_derate_offset_ms,
  CAST(SUM(command_stat.derate_instances) AS BIGINT) AS total_derate_instances
FROM exploded_commands
GROUP BY
  date,
  org_id,
  device_id
"""


COLUMNS = [
    # Primary key columns
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    # Command statistics array (includes bus_id for each command)
    Column(
        name="command_stats",
        type=array_of(
            struct_of(
                ("bus_id", DataType.INTEGER),
                ("request_id", DataType.STRING),
                ("response_id", DataType.STRING),
                ("data_identifier", DataType.STRING),
                ("request_count", DataType.LONG),
                ("any_response_count", DataType.LONG),
                ("derate_offset_ms", DataType.LONG),
                ("derate_instances", DataType.LONG),
            ),
            contains_null=False,
        ),
        nullable=False,
        metadata=Metadata(
            comment="Array of command statistics from all buses with request/response data and derate metrics for each command"
        ),
    ),
    # Summary statistics
    Column(
        name="total_commands",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total number of unique commands processed for this device and bus"
        ),
    ),
    Column(
        name="total_request_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Sum of all requests sent across all commands"),
    ),
    Column(
        name="total_any_response_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Sum of all responses received across all commands"),
    ),
    Column(
        name="total_derate_offset_ms",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Sum of all derate offset times in milliseconds across all commands"
        ),
    ),
    Column(
        name="total_derate_instances",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Sum of all derate instances across all commands"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="OBD command scheduler statistics with arrays of command data per device to minimize row explosion. "
        "Processes protobuf data from Kinesis streams to provide insights into diagnostic command performance and scheduler health. "
        "Each row contains all commands from all buses for a device/date as structured arrays plus summary statistics. "
        "Commands include bus_id within each command struct. Enables efficient analysis of OBD communication patterns, response rates, and command derating behavior.",
        row_meaning="Daily command scheduler statistics for a device, with arrays of individual command metrics from all buses and summary totals",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_COMMAND_SCHEDULER_STATS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_COMMAND_SCHEDULER_STATS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_command_scheduler_stats(context: AssetExecutionContext) -> str:
    """
    Process OBD command scheduler statistics into array-based format.

    This asset transforms nested protobuf command scheduler data from Kinesis into
    efficient array-based structures to minimize row explosion while preserving
    detailed command-level statistics. Each device/date combination gets one row
    with arrays of all command data from all buses.

    Key transformations:
    - Explodes nested command_stats arrays and restructures into command objects
    - Includes bus_id within each command struct for full context
    - Converts binary identifiers to hex format for readability
    - Collects all commands from all buses per device into structured arrays
    - Provides summary statistics alongside detailed command arrays
    - Groups by device and date only (maximum row reduction)

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    return format_date_partition_query(QUERY, context)
