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
    Metric,
    MetricEnum,
    StrEnum,
    check_unique_metric_strings,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import KinesisStats, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)


class TableDimension(StrEnum):
    DATA_IDENTIFIER = "data_identifier"
    IS_STOPPED = "is_stopped"
    EXPIRATION_OFFSET_MS = "expiration_offset_ms"
    REQUEST_COUNT = "request_count"
    ANY_RESPONSE_COUNT = "any_response_count"
    TOTAL_RESPONSE_COUNT = "total_response_count"


class TableMetric(MetricEnum):
    DATA_IDENTIFIER = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_COMMAND_SCHEDULER_STATS,
        field=TableDimension.DATA_IDENTIFIER,
        label="command_scheduler_data_identifier",
    )
    IS_STOPPED = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_COMMAND_SCHEDULER_STATS,
        field=TableDimension.IS_STOPPED,
        label="command_scheduler_is_stopped",
    )
    EXPIRATION_OFFSET_MS = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_COMMAND_SCHEDULER_STATS,
        field=TableDimension.EXPIRATION_OFFSET_MS,
        label="command_scheduler_expiration_offset_ms",
    )
    REQUEST_COUNT = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_COMMAND_SCHEDULER_STATS,
        field=TableDimension.REQUEST_COUNT,
        label="command_scheduler_request_count",
    )
    ANY_RESPONSE_COUNT = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_COMMAND_SCHEDULER_STATS,
        field=TableDimension.ANY_RESPONSE_COUNT,
        label="command_scheduler_any_response_count",
    )
    TOTAL_RESPONSE_COUNT = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_COMMAND_SCHEDULER_STATS,
        field=TableDimension.TOTAL_RESPONSE_COUNT,
        label="command_scheduler_total_response_count",
    )


check_unique_metric_strings(TableMetric)


SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.TIME,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    ColumnType.VALUE,
    Column(
        name=TableDimension.DATA_IDENTIFIER,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The data identifier associated with the command."),
    ),
    Column(
        name=TableDimension.IS_STOPPED,
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Indicates whether the command was stopped during the trip segment."
        ),
    ),
    Column(
        name=TableDimension.EXPIRATION_OFFSET_MS,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="The expiration offset of the command in milliseconds."
        ),
    ),
    Column(
        name=TableDimension.REQUEST_COUNT,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The number of requests for the command."),
    ),
    Column(
        name=TableDimension.ANY_RESPONSE_COUNT,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The number of responses for the command."),
    ),
    Column(
        name=TableDimension.TOTAL_RESPONSE_COUNT,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The total number of responses for the command."),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

# NOTE there is a GROUP BY ALL in this query even though we are extracting
# timestamps. We were having DQ checks fail due to multiple stats with the
# same timestamp
QUERY = """

WITH
    stats AS (
        SELECT
            date
            , time
            , org_id
            , object_id AS device_id
            , value.proto_value.command_scheduler_stats.bus_id AS bus_id
            , COALESCE(stats.is_stopped, FALSE) AS is_stopped
            , stats.command.request_id
            , stats.command.response_id
            , stats.command.data_identifier
            , stats.expiration_stat.expiration_offset_ms
            , stats.request_response_stat.request_count
            , stats.request_response_stat.any_response_count
            , stats.request_response_stat.total_response_count

        FROM kinesisstats.osdcommandschedulerstats

        LATERAL VIEW EXPLODE(value.proto_value.command_scheduler_stats.command_stats) AS stats

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"
            AND NOT value.is_end
            AND NOT value.is_databreak
            -- Filter out commands with no responses because we don't have a
            -- good way to otherwise filter commands for which we never receive
            -- a response.
            AND stats.request_response_stat.any_response_count IS NOT NULL
    )

SELECT
    date
    , time
    , org_id
    , device_id
    , CAST(bus_id AS long) AS bus_id
    , CAST(request_id AS long) AS request_id
    , CAST(response_id AS long) AS response_id
    , CAST(NULL AS long) AS obd_value
    , CAST(NULL AS double) AS value
    , data_identifier
    , is_stopped
    , expiration_offset_ms
    , request_count
    , any_response_count
    , total_response_count

FROM
    stats

GROUP BY ALL
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Extract commands per network per device and any associated statistics",
        row_meaning="A command for a given network/device combo",
        table_type=TableType.STAGING,
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
    # This appears to be superseded by the backfill_batch_size in the schedule definition.
    backfill_batch_size=5,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_COMMAND_SCHEDULER_STATS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_osdcommandschedulerstats(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
