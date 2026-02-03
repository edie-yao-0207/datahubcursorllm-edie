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
from dataweb.userpkgs.firmware.table import (
    Definitions,
    KinesisStats,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)


class TableDimension(StrEnum):
    DELAY_MS = "delay_ms"


class TableMetric(MetricEnum):
    DELAY_MS = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_OBD_VALUE_LOCK_ADDED,
        field=TableDimension.DELAY_MS,
        label="obd_value_lock_added_delay_ms",
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
        name=TableDimension.DELAY_MS,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="The delay in milliseconds for the OBD value lock added."
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
        , defs.bus_id
        , TRY_CAST(value.proto_value.obd_value_lock_added.obd_value AS LONG) AS obd_value
        , TRY_CAST(value.proto_value.obd_value_lock_added.metadata.delay_ms AS DOUBLE) AS delay_ms

    FROM
        kinesisstats.osdobdvaluelockadded

    LEFT JOIN definitions.diagnostic_bus_to_bus_id_mapping AS defs
    ON
        TRY_CAST(value.proto_value.obd_value_lock_added.obd_value_source_lock.vehicle_diagnostic_bus AS BIGINT) = defs.diagnostic_bus_id

    WHERE
        date BETWEEN "{date_start}" AND "{date_end}"
)

SELECT
    date
    , time
    , org_id
    , device_id
    , bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , obd_value
    , CAST(NULL AS DOUBLE) AS value
    , delay_ms

FROM
    stats

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Extract OBD source locks per type of signal. An individiual signal may have multiple ECU sources relaying information.",
        row_meaning="OBD soruce locks by device/signal",
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
        AnyUpstream(KinesisStats.OSD_OBD_VALUE_LOCK_ADDED),
        AnyUpstream(Definitions.DIAGNOSTIC_BUS_TO_BUS_ID_MAPPING),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_OSD_OBD_VALUE_LOCK_ADDED.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_osdobdvaluelockadded(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]

    return QUERY.format(
        date_start=partition_keys[0],
        date_end=partition_keys[-1],
    )
