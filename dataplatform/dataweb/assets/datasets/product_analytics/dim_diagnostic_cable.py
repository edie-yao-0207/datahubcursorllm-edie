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
    ColumnType.CABLE_ID,
)


NON_NULLABLE_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")


QUERY = """

WITH
    prep AS (
        SELECT
            CAST(date AS DATE) AS date
            , org_id
            , object_id as device_id
            , value.int_value AS cable_id

        FROM
            kinesisstats.osdobdcableid

        WHERE
            date BETWEEN date_sub("{date_start}", 7) and date_add("{date_end}", 7)
    )

    , cables AS (
        SELECT
            date
            , org_id
            , device_id
            , cable_id
            , COUNT(*) OVER (PARTITION BY org_id, device_id, cable_id ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS cable_count

        FROM
            prep
    )

SELECT
    CAST(date AS STRING) AS date
    , org_id
    , device_id
    , MAX((cable_count, cable_id)).cable_id as cable_id

FROM
    cables

WHERE
    date between "{date_start}" and "{date_end}"

GROUP BY ALL

"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Derived the connected diagnostic cable type of a device. Cable IDs may misreport, find the most likely cable ID.",
        row_meaning="Diagnostic cable attached to devices.",
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
        AnyUpstream(KinesisStats.OSD_OBD_CABLE_ID),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=3,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalytics.DIM_DIAGNOSTIC_CABLE.value,
        primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
        non_null_keys=NON_NULLABLE_KEYS,
        block_before_write=True,
    ),
)
def dim_diagnostic_cable(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
