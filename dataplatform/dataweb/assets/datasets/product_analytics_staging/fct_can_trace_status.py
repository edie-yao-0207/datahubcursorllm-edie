"""
CAN Trace Status

This table provides comprehensive status information for CAN traces that are available in the 
sensor replay library. Queries DynamoDB directly to retrieve trace metadata including timing, 
conversion status, user information, and tag associations. Each row represents a unique CAN 
trace with deduplication logic applied. The table supports downstream trace selection and 
analysis workflows by providing centralized access to trace availability and metadata without 
requiring direct DynamoDB queries.
"""

from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.constants import (
    AWSRegion,
    Database,
    FRESHNESS_SLO_9AM_PST,
    FIRMWAREVDP,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    array_of,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    Dynamodb,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_query
from dataweb.userpkgs.utils import build_table_description

from .fct_can_recording_windows import (
    START_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
)

QUERY = """
WITH sensor_replay_can_library_records AS (
    SELECT
        srt.TraceUuid
    FROM dynamodb.sensor_replay_traces srt
    WHERE srt.ItemTypeAndIdentifiers = 'collection#library#CAN'
),

can_library_records AS (
    SELECT
        CAST(srt.SourceOrgId AS BIGINT) AS org_id,
        CAST(srt.SourceDeviceId AS BIGINT) AS device_id,
        CAST(srt.SourceAssetMs AS BIGINT) AS start_time,
        CASE 
            WHEN srt.ConvertedAtMs IS NOT NULL AND srt.ConvertedAtMs != "0"
            THEN CAST(srt.ConvertedAtMs AS BIGINT)
            ELSE NULL
        END as converted_at_ms,
        CASE 
            WHEN srt.RetrievedAtMs IS NOT NULL AND srt.RetrievedAtMs != "0"
            THEN CAST(srt.RetrievedAtMs AS BIGINT)
            ELSE NULL
        END as retrieved_at_ms,
        CASE 
            WHEN srt.DurationMs IS NOT NULL AND srt.DurationMs != "0"
            THEN CAST(srt.DurationMs AS BIGINT)
            ELSE NULL
        END as capture_duration,
        srt.TraceUuid as trace_uuid,
        srt.Description as description,
        srt.AvailableSensorTypes as available_sensor_types,
        srt.CollectionType as collection_type,
        srt.CollectionName as collection_name,
        CASE 
            WHEN srt.UserId IS NOT NULL AND srt.UserId != "0"
            THEN CAST(srt.UserId AS BIGINT)
            ELSE NULL
        END as user_id,
        -- Row number used to remove duplicate traces
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

sensor_replay_tag_records AS (
    SELECT
        srt.TraceUuid AS trace_uuid,
        COLLECT_SET(srt.CollectionName) AS tag_names
    FROM dynamodb.sensor_replay_traces srt
    WHERE srt.ItemTypeAndIdentifiers LIKE 'collection#tag#%'
    GROUP BY ALL
)

SELECT 
    DATE_FORMAT(DATE(FROM_UNIXTIME(clr.start_time / 1000)), 'yyyy-MM-dd') as date,
    clr.trace_uuid,
    clr.org_id,
    clr.device_id,
    clr.start_time,
    clr.capture_duration,
    clr.converted_at_ms,
    clr.description,
    clr.available_sensor_types,
    clr.collection_type,
    clr.collection_name,
    clr.user_id,
    COALESCE(clr.converted_at_ms != 0, FALSE) AS is_available,
    srt.tag_names
FROM can_library_records clr
LEFT JOIN sensor_replay_tag_records srt USING (trace_uuid)
WHERE clr.row_num = 1  -- Remove duplicate traces
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.TRACE_UUID,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    START_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
    Column(
        name="converted_at_ms",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Timestamp when trace was converted in milliseconds since epoch (ConvertedAtMs from DynamoDB)"
        ),
    ),
    Column(
        name="description",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="User-provided description of the trace"),
    ),
    Column(
        name="available_sensor_types",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="JSON string of available sensor types in the trace"),
    ),
    Column(
        name="collection_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Type of collection this trace belongs to"),
    ),
    Column(
        name="collection_name",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Name of collection this trace belongs to"),
    ),
    Column(
        name="user_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="ID of user who created the trace"),
    ),
    Column(
        name="is_available",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether trace has CAN data available in library (collection#library#CAN record exists)"
        ),
    ),
    Column(
        name="tag_names",
        type=array_of(DataType.STRING),
        nullable=True,
        metadata=Metadata(comment="Name of tag this trace belongs to"),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Comprehensive status information for CAN traces available in the sensor replay library. "
        "Queries DynamoDB sensor_replay_traces directly to provide trace metadata including timing, conversion status, "
        "user information, and tag associations. Features deduplication logic for traces with multiple UUIDs and "
        "aggregated tag collection. Date partitioned by trace start time to support efficient querying. "
        "Enables downstream trace selection and analysis by centralizing trace availability and metadata access.",
        row_meaning="Each row represents a unique CAN trace available in the sensor replay library with comprehensive metadata and status information.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(Dynamodb.SENSOR_REPLAY_TRACES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CAN_TRACE_STATUS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_can_trace_status(context: AssetExecutionContext) -> str:
    return format_query(QUERY)
