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
    PROTOCOL = "protocol"
    PROTOCOL_COUNT = "protocol_count"
    PROTOCOL_SELECTED_TO_RUN = "protocol_selected_to_run"


class TableMetric(MetricEnum):
    DETECTED_PROTOCOL = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_CAN_PROTOCOLS_DETECTED,
        field=TableDimension.PROTOCOL,
        label="can_protocol_detected",
    )
    DETECTED_PROTOCOL_COUNT = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_CAN_PROTOCOLS_DETECTED,
        field=TableDimension.PROTOCOL_COUNT,
        label="can_protocol_detected_count",
    )
    PROTOCOL_SELECTED_TO_RUN = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_CAN_PROTOCOLS_DETECTED,
        field=TableDimension.PROTOCOL_SELECTED_TO_RUN,
        label="can_protocol_selected_to_run",
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
        name=TableDimension.PROTOCOL,
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="The protocol detected on the vehicle diagnostic bus."
        ),
    ),
    Column(
        name=TableDimension.PROTOCOL_SELECTED_TO_RUN,
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="The protocol selected to run on the vehicle diagnostic bus."
        ),
    ),
    Column(
        name=TableDimension.PROTOCOL_COUNT,
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="The number of protocols detected on the vehicle diagnostic bus."
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

WITH
    stats AS (
        SELECT
            date
            , time
            , org_id
            , object_id AS device_id
            , value.proto_value.can_protocols_detected.bus_id AS bus_id
            , entry.protocol AS protocol
            , CASE
                WHEN entry.selected_to_run THEN entry.protocol
                ELSE NULL
                END AS protocol_selected_to_run
            , array_size(value.proto_value.can_protocols_detected.detected_can_protocols) AS protocol_count

        FROM
            kinesisstats.osdcanprotocolsdetected

        LATERAL VIEW
            EXPLODE(value.proto_value.can_protocols_detected.detected_can_protocols) AS entry

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"
            AND NOT value.is_end
            AND NOT value.is_databreak
    )

SELECT
    date
    , time
    , org_id
    , device_id
    , CAST(bus_id AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(NULL AS DOUBLE) AS value
    , protocol
    , protocol_count
    , protocol_selected_to_run

FROM
    stats

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Extract bitrate detected per device network interface.",
        row_meaning="Bitrates by device network",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_CAN_PROTOCOLS_DETECTED),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_CAN_PROTOCOLS_DETECTED.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_osdcanprotocolsdetected(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
