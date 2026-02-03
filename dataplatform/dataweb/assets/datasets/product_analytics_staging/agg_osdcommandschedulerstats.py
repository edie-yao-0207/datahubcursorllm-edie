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
from dataweb.userpkgs.firmware.metric import Metric, MetricEnum, StrEnum
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import KinesisStats, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import (
    create_run_config_overrides,
    format_date_partition_query,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)


class TableDimension(StrEnum):
    EXPIRED = "expired"
    STOPPED = "stopped"


class TableMetric(MetricEnum):
    EXPIRED = Metric(
        type=ProductAnalyticsStaging.AGG_OSD_COMMAND_SCHEDULER_STATS,
        field=TableDimension.EXPIRED,
        label="command_scheduler_expired",
    )
    STOPPED = Metric(
        type=ProductAnalyticsStaging.AGG_OSD_COMMAND_SCHEDULER_STATS,
        field=TableDimension.STOPPED,
        label="command_scheduler_stopped",
    )


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
        name=TableDimension.STOPPED,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Indicates whether the command was stopped during the trip segment."
        ),
    ),
    Column(
        name=TableDimension.EXPIRED,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Indicates whether the command expired during the trip segment."
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

WITH
    can_connections AS (
        SELECT
            date
            , org_id
            , object_id AS device_id
            , NAMED_STRUCT("state", value.int_value, "ms", time) AS start
            , LEAD(NAMED_STRUCT("state", value.int_value, "ms", time)) OVER (PARTITION BY org_id, object_id ORDER BY time ASC) AS end

        FROM
            kinesisstats.osdcanconnected

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"
    )

    , connection_state AS (
        SELECT
            *
            , end.ms - start.ms AS time_diff

        FROM
            can_connections

        WHERE
            -- CONNECT
            start.state = 2
            -- DISCONNECT
            AND end.state = 3
            AND (end.ms - start.ms) > 3600000
    )

    , stats_on_trip AS (
        SELECT
            k.date
            , k.time
            , k.org_id
            , k.object_id AS device_id
            , c.start.ms AS start_connection_state_ms
            , value.proto_value.command_scheduler_stats.bus_id AS bus_id
            , coalesce(stats.is_stopped, false) AS is_stopped
            , stats.command.request_id
            , stats.command.data_identifier
            , stats.expiration_stat.expiration_offset_ms

        FROM
            kinesisstats.osdcommandschedulerstats AS k

        JOIN
            connection_state AS c
            ON k.date = c.date
            AND k.org_id = c.org_id
            AND k.object_id = c.device_id

        LATERAL VIEW
            EXPLODE(value.proto_value.command_scheduler_stats.command_stats) AS stats

        WHERE
            -- only consider stats logged during the trip segment
            k.time BETWEEN c.start.ms AND c.end.ms
    )

    , stats_by_unique_id AS (
        SELECT
            date
            , device_id
            , org_id
            , bus_id
            , request_id
            , data_identifier
            , max(expiration_offset_ms > 0) AS expired
            , max(is_stopped) AS stopped

        FROM
            stats_on_trip

        GROUP BY ALL
    )

    , command_set AS (
        SELECT
            date
            , device_id
            , org_id
            , bus_id
            , COLLECT_SET(STRUCT(stopped, expired, bus_id, request_id, data_identifier)) AS unique_commands

        FROM
            stats_by_unique_id

        GROUP BY ALL
    )

SELECT
    date
    , CAST(NULL AS LONG) AS time
    , org_id
    , device_id
    , CAST(bus_id AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(NULL AS DOUBLE) AS value
    , CAST(COUNT(DISTINCT(FILTER(unique_commands, x -> x.stopped))) = 1 AS DOUBLE) AS stopped
    , CAST(COUNT(DISTINCT(FILTER(unique_commands, x -> x.expired))) = 1 AS DOUBLE) AS expired

FROM
    command_set

GROUP BY ALL

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc=(
            "Check for commanded diagnostic failure rates when communicating with a vehicle."
            "This metric indicates how many commands per device are not getting responses."
            "Indicates either an OEM issue with standard diagnostic responses, or a promotion has been applied to a vehicle incorrectly."
        ),
        row_meaning="Diagnostic failure rates per device",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_CAN_CONNECTED),
        AnyUpstream(KinesisStats.OSD_COMMAND_SCHEDULER_STATS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_OSD_COMMAND_SCHEDULER_STATS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_osdcommandschedulerstats(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
