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
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)

class TableDimension(StrEnum):
    PROBE_TO_LOOP_START_DURATION_MAX = "probe_to_loop_start_duration_max"
    SCHEDULER_NO_COMMAND = "scheduler_no_command"
    MESSAGES_WRITTEN = "messages_written"
    MESSAGES_DROPPED = "messages_dropped"
    SYNC_TIMEOUT = "sync_timeout"


class TableMetric(MetricEnum):
    PROBE_TO_LOOP_START_DURATION_MAX = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_FIRMWARE_METRICS_VDP,
        field=TableDimension.PROBE_TO_LOOP_START_DURATION_MAX,
        label="obd_probe_to_loop_start_duration_max",
    )
    SCHEDULER_NO_COMMAND = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_FIRMWARE_METRICS_VDP,
        field=TableDimension.SCHEDULER_NO_COMMAND,
        label="diagnostic_scheduler_no_command",
    )
    MESSAGES_WRITTEN = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_FIRMWARE_METRICS_VDP,
        field=TableDimension.MESSAGES_WRITTEN,
        label="diagnostic_messages_written",
    )
    MESSAGES_DROPPED = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_FIRMWARE_METRICS_VDP,
        field=TableDimension.MESSAGES_DROPPED,
        label="diagnostic_messages_dropped",
    )
    SYNC_TIMEOUT = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_FIRMWARE_METRICS_VDP,
        field=TableDimension.SYNC_TIMEOUT,
        label="diagnostic_sync_timeout",
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
        name=TableDimension.PROBE_TO_LOOP_START_DURATION_MAX,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="The maximum duration in milliseconds from probe to loop start."
        ),
    ),
    Column(
        name=TableDimension.SCHEDULER_NO_COMMAND,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="The number of times the scheduler had no command to run."
        ),
    ),
    Column(
        name=TableDimension.MESSAGES_WRITTEN,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="The number of messages written by the bus manager."),
    ),
    Column(
        name=TableDimension.MESSAGES_DROPPED,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="The number of messages dropped by the bus manager."),
    ),
    Column(
        name=TableDimension.SYNC_TIMEOUT,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="The number of times the bus manager sync timed out."
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")


QUERY = """--sql

WITH stats AS (
    SELECT
        date
        , time
        , org_id
        , object_id AS device_id
        , (
            CASE
                WHEN ARRAY_CONTAINS(metric.tags, "BUSID_CAN0") THEN 1
                WHEN ARRAY_CONTAINS(metric.tags, "BUSID_CAN1") THEN 2
                WHEN ARRAY_CONTAINS(metric.tags, "BUSID_CAN2") THEN 3
                WHEN ARRAY_CONTAINS(metric.tags, "BUSID_KLINE") THEN 4
                WHEN ARRAY_CONTAINS(metric.tags, "BUSID_J1708") THEN 5
                WHEN ARRAY_CONTAINS(metric.tags, "BUSID_J1850") THEN 6
                ELSE 0
            END
        ) AS bus_id
        , metric.name AS field
        , COALESCE(metric.value, 0) AS value

    FROM
        kinesisstats.osdfirmwaremetrics

    LATERAL VIEW
        EXPLODE(value.proto_value.firmware_metrics.metrics) AS metric

    WHERE
        date BETWEEN "{date_start}" AND "{date_end}"
        AND NOT value.is_end
        AND NOT value.is_databreak
        AND metric.name IN (
            "obd.uvl.probe_to_loop_start_duration_ms.max",
            "obd.uvl.probe_to_loop_start_duration_ms.max",
            "obd.scheduler.no_command",
            "obd.busmanager.messages_written",
            "obd.busmanager.messages_dropped",
            "obd.busmanager.sync_timeout"
        )
)

, data AS (
    SELECT
        date
        , field
        , time
        , org_id
        , device_id
        , CAST(bus_id AS LONG) AS bus_id
        , CAST(SUM(value) AS DOUBLE) AS value

    FROM
        stats

    GROUP BY ALL
)

SELECT
    date
    , time
    , org_id
    , device_id
    , bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(NULL AS DOUBLE) AS value
    , probe_to_loop_start_duration_max
    , scheduler_no_command
    , messages_written
    , messages_dropped
    , sync_timeout

FROM
    data

PIVOT (
    SUM(value) FOR field IN (
        "obd.uvl.probe_to_loop_start_duration_ms.max" AS probe_to_loop_start_duration_max,
        "obd.scheduler.no_command" AS scheduler_no_command,
        "obd.busmanager.messages_written" AS messages_written,
        "obd.busmanager.messages_dropped" AS messages_dropped,
        "obd.busmanager.sync_timeout" AS sync_timeout
    )
)


"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Extract firmware metrics to individual rows per firmware metric.",
        row_meaning="Firmware metric readings",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_FIRMWARE_METRICS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_FIRMWARE_METRICS_VDP.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_osdfirmwaremetrics_vdp(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
