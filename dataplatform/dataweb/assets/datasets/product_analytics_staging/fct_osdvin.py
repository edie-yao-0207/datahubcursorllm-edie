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
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
    ProductArea,
    SourceLink,
)
from dataweb.userpkgs.firmware.metric import (
    Metric,
    MetricEnum,
    StatMetadata,
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
    VIN_COUNT = "vin_count"
    UNIQUE_VIN_COUNT = "unique_vin_count"


class TableMetric(MetricEnum):
    VIN_COUNT = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_VIN,
        field=TableDimension.VIN_COUNT,
        label="vin_count",
        metadata=StatMetadata(
            product_area=ProductArea.VEHICLE_DIAGNOSTICS,
            sub_product_area=ProductArea.US_COMPLIANCE,
            signal_name="Vehicle Identification Number (VIN)",
            default_priority=1,
            is_applicable_ice=True,
            is_applicable_hydrogen=True,
            is_applicable_hybrid=True,
            is_applicable_phev=True,
            is_applicable_bev=True,
            is_applicable_unknown=True,
        ),
    )
    UNIQUE_VIN_COUNT = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_VIN,
        field=TableDimension.UNIQUE_VIN_COUNT,
        label="unique_vin_count",
        metadata=StatMetadata(
            product_area=ProductArea.VEHICLE_DIAGNOSTICS,
            sub_product_area=ProductArea.US_COMPLIANCE,
            signal_name="Unique VIN Count",
            default_priority=1,
            is_applicable_ice=True,
            is_applicable_hydrogen=True,
            is_applicable_hybrid=True,
            is_applicable_phev=True,
            is_applicable_bev=True,
            is_applicable_unknown=True,
        ),
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
        name=TableDimension.VIN_COUNT,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="The number of VINs seen on the device."),
    ),
    Column(
        name=TableDimension.UNIQUE_VIN_COUNT,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="The number of unique VINs seen on the device."),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

WITH
    data as (
        SELECT
            date
            , org_id
            , object_id as device_id
            , COUNT(value.proto_value.vin_event.vin) AS vin_count
            , COUNT(DISTINCT value.proto_value.vin_event.vin) AS unique_vin_count
        FROM
            kinesisstats.osDVin
        WHERE
            date BETWEEN DATE("{date_start}") AND DATE("{date_end}")
        GROUP BY ALL
    )

SELECT
    date
    , CAST(NULL AS LONG) AS time
    , org_id
    , device_id
    , CAST(NULL AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(NULL AS DOUBLE) AS value
    , vin_count
    , unique_vin_count
FROM
    data

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Statistics on VINs seen per device. This is based on VINs that have already been filtered on the firmware based on ISO standard for VINs",
        row_meaning="VIN stats for a given device",
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
        AnyUpstream(KinesisStats.OSD_VIN),
        AnyUpstream(KinesisStats.OSD_DIAGNOSTIC_MESSAGES_SEEN),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_VIN.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_osdvin(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
