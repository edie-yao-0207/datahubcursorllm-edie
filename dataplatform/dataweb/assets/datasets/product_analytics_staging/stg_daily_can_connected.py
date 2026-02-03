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
from dataweb.userpkgs.utils import build_table_description

SCHEMA = columns_to_schema(
    ColumnType.DATE_TYPED_DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="count_canbus_connect",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Count of CANBUS_CONNECT events.",
        ),
    ),
    Column(
        name="count_canbus_disconnect",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Count of CANBUS_DISCONNECT events.",
        ),
    ),
    Column(
        name="count_usb_connect",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Count of USB_CONNECT events.",
        ),
    ),
    Column(
        name="count_usb_disconnect",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Count of USB_DISCONNECT events.",
        ),
    ),
    Column(
        name="count_total",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Total count of events.",
        ),
    ),
)

QUERY = """

SELECT
    DATE(stat.date) AS date
    , stat.org_id
    , stat.object_id AS device_id
    , COUNT_IF(name.name = 'CANBUS_CONNECT') AS count_canbus_connect
    , COUNT_IF(name.name = 'CANBUS_DISCONNECT') AS count_canbus_disconnect
    , COUNT_IF(name.name = 'USB_CONNECT') AS count_usb_connect
    , COUNT_IF(name.name = 'USB_DISCONNECT') AS count_usb_disconnect
    , COUNT(*) AS count_total

FROM
    kinesisstats.osDCanConnected stat

INNER JOIN
    definitions.can_bus_events name
    ON name.id = stat.value.int_value

WHERE
    stat.date BETWEEN "{FIRST_PARTITION_START}" AND "{FIRST_PARTITION_END}"

GROUP BY ALL

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily CAN connected data",
        row_meaning="Connected CAN stats",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_CAN_CONNECTED),
        AnyUpstream(Definitions.CAN_BUS_EVENTS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.STG_DAILY_CAN_CONNECTED.value,
        primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
        non_null_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
        block_before_write=True,
    ),
)
def stg_daily_can_connected(context: AssetExecutionContext) -> str:
    return QUERY
