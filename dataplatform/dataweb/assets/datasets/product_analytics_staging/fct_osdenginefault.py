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
    J1939_FAULT_COUNT = "j1939_fault_count"
    PASSENGER_FAULT_COUNT = "passenger_fault_count"


class TableMetric(MetricEnum):
    J1939_FAULT_COUNT = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_ENGINE_FAULT,
        field=TableDimension.J1939_FAULT_COUNT,
        label="j1939_fault_count",
        metadata=StatMetadata(
            product_area=ProductArea.VEHICLE_DIAGNOSTICS,
            sub_product_area=ProductArea.MAINTENANCE,
            signal_name="J1939 Engine Faults",
            default_priority=1,
            is_applicable_ice=True,
            is_applicable_hydrogen=True,
            is_applicable_hybrid=True,
            is_applicable_phev=True,
            is_applicable_bev=True,
            is_applicable_unknown=True,
        ),
    )
    PASSENGER_FAULT_COUNT = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_ENGINE_FAULT,
        field=TableDimension.PASSENGER_FAULT_COUNT,
        label="passenger_fault_count",
        metadata=StatMetadata(
            product_area=ProductArea.VEHICLE_DIAGNOSTICS,
            sub_product_area=ProductArea.MAINTENANCE,
            signal_name="Passenger Engine Faults",
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
        name=TableDimension.J1939_FAULT_COUNT,
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="The number of J1939 faults detected on the vehicle diagnostic bus."
        ),
    ),
    Column(
        name=TableDimension.PASSENGER_FAULT_COUNT,
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="The number of passenger faults detected on the vehicle diagnostic bus."
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
            , ARRAY_SIZE(value.proto_value.vehicle_fault_event.j1939_faults) AS j1939_fault_count
            , ARRAY_SIZE(value.proto_value.vehicle_fault_event.passenger_faults) AS passenger_fault_count

        FROM
            kinesisstats.osdenginefault

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
    , CAST(NULL AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(NULL AS DOUBLE) AS value
    , j1939_fault_count
    , passenger_fault_count

FROM
    stats

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Extract the number of faults seen from J1979 or J1939.",
        row_meaning="A fault from a J1979/J1939",
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
        AnyUpstream(KinesisStats.OSD_ENGINE_FAULT),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_ENGINE_FAULT.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_osdenginefault(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
