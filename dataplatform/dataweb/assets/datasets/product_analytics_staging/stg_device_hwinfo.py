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
from dataweb.userpkgs.firmware.table import KinesisStats, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.GATEWAY_ID,
    ColumnType.PRODUCT_VERSION_STR,
    ColumnType.BOM_VERSION,
)

PRIMARY_KEYS = columns_to_names(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.GATEWAY_ID,
)

NON_NULLABLE_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """
SELECT
  date
  , org_id
  , object_id AS gateway_id
  , MAX(value.proto_value.hardware_info.product_version) AS product_version_str
  , MAX(CAST(value.proto_value.hardware_info.bill_of_materials_version AS LONG)) AS bom_version
FROM kinesisstats.osghardwareinfo
WHERE 
  date BETWEEN "{date_start}" AND "{date_end}"
  AND NOT value.is_databreak
  AND NOT value.is_end
GROUP BY ALL
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="VG54 MCU version information per device per day. This contains relevant information around the hardware used in the production of a VG54. Differences include board revisions such as change in hardware network controllers for diagnostics.",
        row_meaning="VG54 information for a given device",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[AnyUpstream(KinesisStats.OSG_HARDWARE_INFO)],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=3,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.STG_DEVICE_HWINFO.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULLABLE_COLUMNS,
        block_before_write=True,
    ),
    priority=5,
)
def stg_device_hwinfo(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]

    return QUERY.format(
        date_start=partition_keys[0],
        date_end=partition_keys[-1],
    )
