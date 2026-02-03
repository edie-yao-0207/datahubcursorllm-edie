from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    PRIMARY_KEYS_DATE_ORG_DEVICE,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
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
from dataweb.userpkgs.firmware.constants import ProductArea
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description


class TableDimension(StrEnum):
    FAULT_COUNT = "fault_count"


class TableMetric(MetricEnum):
    FAULT_COUNT = Metric(
        type=ProductAnalyticsStaging.STG_DAILY_FAULTS,
        field=TableDimension.FAULT_COUNT,
        label="diagnostic_fault_count",
        metadata=StatMetadata(
            product_area=ProductArea.VEHICLE_DIAGNOSTICS,
            sub_product_area=ProductArea.MAINTENANCE,
            signal_name="Diagnostic Fault Count",
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
        name=TableDimension.FAULT_COUNT,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Count of faults seen.",
        ),
    ),
)


QUERY = """--sql

SELECT
    date
    , CAST(NULL AS LONG) AS time
    , org_id
    , object_id as device_id
    , CAST(NULL AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(NULL AS DOUBLE) AS value
    , CAST(SUM(COALESCE(ARRAY_SIZE(FILTER(value.proto_value.diagnostic_messages_seen, x -> x.msg_id IN (257, 2290689, 3, 7, 10, 108484824094, 1660211, 65226))), 0)) AS DOUBLE) AS fault_count

FROM
    kinesisstats.osddiagnosticmessagesseen

WHERE
    date BETWEEN DATE("{FIRST_PARTITION_START}") AND DATE("{FIRST_PARTITION_END}")

GROUP BY ALL

--endsql
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc=(
            "Daily faults data produced per vehicle. A fault is counted if we detect the presence of a diagnostic response for fault data."
            "This applies to J1939, J1979, and ISO27145."
        ),
        row_meaning="Fault stats for a given vehicle",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_DIAGNOSTIC_MESSAGES_SEEN),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.STG_DAILY_FAULTS.value,
        primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
        non_null_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
        block_before_write=True,
    ),
)
def stg_daily_faults(context: AssetExecutionContext) -> str:
    return QUERY
