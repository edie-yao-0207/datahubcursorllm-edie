from dataclasses import replace
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.metric import (
    StrEnum,
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
from dataweb.userpkgs.firmware.table import KinesisStatsHistory, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)

class TableDimension(StrEnum):
    MESSAGE_ID = "message_id"
    TX_ID = "tx_id"
    AVG_LAST_REPORTED_AGO_MS = "avg_last_reported_ago_ms"
    SUM_TOTAL_COUNT = "sum_total_count"

COLUMNS = [
    ColumnType.DATE_TYPED_DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    replace(ColumnType.BUS_ID.value, primary_key=True),
    replace(ColumnType.BUS_NAME.value, primary_key=True),
    Column(
        name=TableDimension.TX_ID,
        type=DataType.LONG,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment="Indicates whether the command was stopped during the trip segment."
        ),
    ),
    Column(
        name=TableDimension.MESSAGE_ID,
        type=DataType.LONG,
        nullable=True,
        primary_key=True,
        metadata=Metadata(comment="The data identifier associated with the command."),
    ),
    Column(
        name=TableDimension.AVG_LAST_REPORTED_AGO_MS,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="The average last reported timestamp of a dataframe."),
    ),
    Column(
        name=TableDimension.SUM_TOTAL_COUNT,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The total count of frames seen."),
    )
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
WITH

data AS (
    SELECT
        DATE(date) AS date
        , time
        , org_id
        , object_id AS device_id
        , INLINE(value.proto_value.diagnostic_messages_seen)
    FROM
        kinesisstats_history.osDDiagnosticMessagesSeen
    WHERE
        date BETWEEN "{date_start}" AND "{date_end}"
        AND NOT value.is_databreak
        AND NOT value.is_end
)

, grouped AS (
    SELECT
        date
        , org_id
        , device_id
        , COALESCE(bus_type, 0) AS bus_id
        , COALESCE(bus.name, 'Unknown') AS bus_name
        , COALESCE(txid, 0) AS tx_id
        , COALESCE(msg_id, 0) AS message_id
        , AVG(COALESCE(last_reported_ago_ms, 0)) AS avg_last_reported_ago_ms
        , SUM(total_count) AS sum_total_count
    FROM
        data
    LEFT JOIN
        definitions.can_bus_types AS bus
        ON COALESCE(data.bus_type, 0) = bus.id
    GROUP BY ALL
)

SELECT * FROM grouped
"""

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Extract commands seen per device and diagnostic bus.",
        row_meaning="A protocol data unit seen per device.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStatsHistory.OSD_DIAGNOSTIC_MESSAGES_SEEN),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_DIAGNOSTIC_MESSAGES_SEEN.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_osddiagnosticmessagesseen(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
