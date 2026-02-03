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
from dataweb.userpkgs.firmware.schema import (
    ColumnType,
    columns_to_names,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import KinesisStats, ProductAnalytics
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.HAS_MODI,
    ColumnType.HAS_USB_SERIAL,
    ColumnType.HAS_FALKO,
    ColumnType.HAS_CM3X,
    ColumnType.HAS_J1708_USB,
    ColumnType.HAS_OCTO,
    ColumnType.HAS_BRIGID,
    ColumnType.HAS_CM2X,
    ColumnType.HAS_DUCKBILL_CABLE,
)


PRIMARY_KEYS = columns_to_names(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
)

NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

WITH
    usbs AS (
        SELECT
            date
            , org_id
            , object_id
            , COLLECT_SET(device.usb_id) AS usb_ids

        FROM
            kinesisstats.osdattachedusbdevices

        LATERAL VIEW EXPLODE(value.proto_value.attached_usb_devices) AS device

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"

        GROUP BY ALL
    )

SELECT
    usbs.date
    , org_id
    , object_id AS device_id
    -- ModiVid = 0x10C4
    -- ModiPid = 0x87A0
    , EXISTS(usb_ids, x -> x == 281315232) AS has_modi
    -- SerialToUsbVid  = 0x403
    -- SerialToUsbPid  = 0x6001
    -- 0x04036001
    -- SerialToUsbVid  = 0x403
    -- SerialToUsbPid2 = 0x6015
    -- 0x04036015
    , EXISTS(usb_ids, x -> x IN (67330049, 67330069)) AS has_usb_serial
    -- FalkoVid = 0x1D50
    -- FalkoPid = 0x606F
    -- 0x1D50606F
    , EXISTS(usb_ids, x -> x == 491806831) AS has_falko
    -- Cm3xVid = 0x636D
    -- Cm3xPid = 0x434D
    -- 0x636D434D
    , EXISTS(usb_ids, x -> x == 1668105037) AS has_cm3x
    -- J1708Vid = 0x1708
    -- J1708Pid = 0x1708
    -- 0x17081708
    , EXISTS(usb_ids, x -> x == 386406152) AS has_j1708_usb
    -- OctoVid = 0x4162
    -- OctoPid = 0x6869
    -- 0x41626869
    , EXISTS(usb_ids, x -> x == 1096968297) AS has_octo
    -- https://samsara-rd.slack.com/archives/C024V8KQTRR/p1701118002527359
    , EXISTS(usb_ids, x -> x == 185272842) AS has_brigid
    -- https://samsara-rd.slack.com/archives/C024N9JJ1CN/p1589819971018800
    , EXISTS(usb_ids, x -> x == 205874022) AS has_cm2x
    -- DuckbillVid = 0x1D50
    -- DuckbillPid = 0x6073
    -- 0x1D506073
    , EXISTS(usb_ids, x -> x == 491806835) AS has_duckbill_cable

FROM
    usbs

"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Asset containing USB devices connected per gateway per day. The connected USB devices detected in this table are speficially other Samsara products. This includes cameras, serial cables, diagnostic cables, and various diagnostic accessories. There is a simple boolean per type of device connected to gateway asset.",
        row_meaning="A given instance of a USB device being connected to a gateway",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[AnyUpstream(KinesisStats.OSD_ATTACHED_USB_DEVICES)],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=3,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalytics.DIM_ATTACHED_USB_DEVICES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    priority=5,
)
def dim_attached_usb_devices(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)