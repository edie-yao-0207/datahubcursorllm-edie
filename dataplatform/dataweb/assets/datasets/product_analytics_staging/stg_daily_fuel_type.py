from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, get_databases, table
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
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)

SCHEMA = columns_to_schema(
    ColumnType.DATE_TYPED_DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.ENGINE_TYPE,
    ColumnType.FUEL_TYPE,
)

NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")


QUERY = """

WITH

    date_sequence AS (
        SELECT
            EXPLODE(SEQUENCE(DATE("{date_start}"), DATE("{date_end}"))) AS date
    )

    , data AS (
        SELECT
            ds.date
            , cft.org_id
            , cft.device_id
            , MAX((cft.segment_start, cft.engine_type_name)).engine_type_name AS engine_type
            , MAX((cft.segment_start, cft.gasoline_type_name)).gasoline_type_name AS gasoline_type
            , MAX((cft.segment_start, cft.diesel_type_name)).diesel_type_name AS diesel_type
            , MAX((cft.segment_start, cft.gaseous_type_name)).gaseous_type_name AS gaseous_type
            , MAX((cft.segment_start, cft.hydrogen_type_name)).hydrogen_type_name AS hydrogen_type
            , MAX((cft.segment_start, cft.battery_charge_type_name)).battery_charge_type_name AS electric_type
        FROM
            date_sequence AS ds

        JOIN
            {product_analytics_staging}.stg_segment_canonical_fuel_types AS cft

        WHERE
            ds.date BETWEEN cft.segment_start AND cft.segment_end
            OR (ds.date > cft.segment_start AND cft.segment_end IS NULL)

        GROUP BY ALL
    )

SELECT
    DATE(date) AS date
    , org_id
    , device_id
    , engine_type
    , concat_ws("_", filter(array(gasoline_type, diesel_type, gaseous_type, hydrogen_type, electric_type), x -> not contains(x, "NA"))) as fuel_type

FROM
    data

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily fuel type data",
        row_meaning="Each row represents the fuel type for a given device",
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
        AnyUpstream(ProductAnalyticsStaging.STG_SEGMENT_CANONICAL_FUEL_TYPES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=30,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.STG_DAILY_FUEL_TYPE.value,
        primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    priority=5,
)
def stg_daily_fuel_type(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]

    return QUERY.format(
        product_analytics_staging=get_databases()[Database.PRODUCT_ANALYTICS_STAGING],
        date_start=partition_keys[0],
        date_end=partition_keys[-1],
    )
