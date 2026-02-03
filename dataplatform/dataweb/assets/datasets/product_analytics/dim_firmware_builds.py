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
from dataweb.userpkgs.firmware.schema import ColumnType, columns_to_schema
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
    ColumnType.BUILD,
)

NON_NULLABLE_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")


QUERY = """

WITH
    builds AS (
        SELECT
            date
            , org_id
            , object_id AS device_id
            , MAX((time, value.proto_value.hub_server_device_heartbeat.connection.device_hello.build AS build)).build AS build

        FROM
            kinesisstats.osdhubserverdeviceheartbeat

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"
            AND NOT value.is_databreak
            AND NOT value.is_end
            AND value.proto_value.hub_server_device_heartbeat.connection.device_hello.build IS NOT NULL

        GROUP BY ALL
    )

SELECT
    date
    , org_id
    , device_id
    , build

FROM
    builds

WHERE
     build IS NOT NULL

"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Determine the firmware build on a device per day. Devices with multiple builds reported will chose the most recently reported build.",
        row_meaning="Firmware build version for a given device",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_HUB_SERVER_DEVICE_HEARTBEAT),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=3,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalytics.DIM_FIRMWARE_BUILDS.value,
        primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
        non_null_keys=NON_NULLABLE_COLUMNS,
        block_before_write=True,
    ),
    priority=5,
)
def dim_firmware_builds(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)