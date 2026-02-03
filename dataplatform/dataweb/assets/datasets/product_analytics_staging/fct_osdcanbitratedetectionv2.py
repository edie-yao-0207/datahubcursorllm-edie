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
    BITRATE = "bitrate"


class TableMetric(MetricEnum):
    BITRATE = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_CAN_BITRATE_DETECTION_V2,
        field=TableDimension.BITRATE,
        label="can_bitrate_detection_v2",
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
        name=TableDimension.BITRATE,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="The bitrate detected on the vehicle diagnostic bus."
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
            , object_id as device_id
            , COALESCE(value.proto_value.can_bitrate_detection_v2_results.bus_id, b.bus_id, 0) as bus_id
            , COALESCE(value.proto_value.can_bitrate_detection_v2_results.detected_bitrate, 0) AS bitrate

        FROM
            kinesisstats.osdcanbitratedetectionv2 a

        LEFT JOIN {product_analytics_staging}.fct_caninterface_busid b
            ON a.value.proto_value.can_bitrate_detection_v2_results.can_interface = b.can_interface

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"
            -- TODO: for now we filter out 0 values until we implement more
            -- advanced filtering based on CAN connections
            AND COALESCE(a.value.proto_value.can_bitrate_detection_v2_results.detected_bitrate, 0) > 0
    )

SELECT
    date
    , time
    , org_id
    , device_id
    , CAST(bus_id AS LONG) AS bus_id
    , CAST(null AS LONG) AS request_id
    , CAST(null AS LONG) AS response_id
    , CAST(null AS LONG) AS obd_value
    , CAST(0 AS double) AS value
    , bitrate AS bitrate

FROM
    stats

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Aggregate bitrates detected per VG network I/O",
        row_meaning="Bitrates by VG network",
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
        AnyUpstream(KinesisStats.OSD_CAN_BITRATE_DETECTION_V2),
        AnyUpstream(ProductAnalyticsStaging.FCT_CANINTERFACE_BUSID),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_CAN_BITRATE_DETECTION_V2.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_osdcanbitratedetectionv2(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
