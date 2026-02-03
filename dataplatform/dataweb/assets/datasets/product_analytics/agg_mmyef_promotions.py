from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, get_databases, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import ColumnType, columns_to_schema
from dataweb.userpkgs.firmware.table import ProductAnalytics, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
    ColumnType.FUEL_TYPE,
    ColumnType.ENGINE_TYPE,
    ColumnType.SIGNAL,
    ColumnType.BUS,
    ColumnType.ELECTRIFICATION_LEVEL,
    ColumnType.PRIMARY_FUEL_TYPE,
    ColumnType.POPULATION_COUNT,
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """
SELECT
    population.date,
    population.make,
    population.model,
    population.year,
    population.engine_model,
    population.fuel_type,
    population.engine_type,
    promotion.signal,
    promotion.bus,
    promotion.electrification_level,
    promotion.primary_fuel_type,
    SUM(population.count_distinct_device_id) AS population_count
FROM
    product_analytics.dim_populations AS population
JOIN
    {product_analytics_staging}.stg_vehicle_promotions AS promotion
    ON (population.population_type = "make model year engine_model fuel_type engine_type")
    AND (promotion.make = "*" OR LOWER(population.make) RLIKE LOWER(promotion.make))
    AND (promotion.model = "*" OR LOWER(population.model) RLIKE LOWER(promotion.model))
    AND (promotion.year = 0 OR promotion.year = population.year)
    AND (promotion.engine_model IS NULL OR LOWER(promotion.engine_model) <=> LOWER(population.engine_model))
    AND (promotion.electrification_level IS NULL OR population.engine_type = promotion.electrification_level)
    AND (promotion.primary_fuel_type IS NULL OR CONTAINS(LOWER(population.fuel_type), LOWER(promotion.primary_fuel_type)))
WHERE
    population.date BETWEEN "{date_start}" AND "{date_end}"
GROUP BY ALL
"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Vehicle promotions from the backend and the populations of devices they apply to.",
        row_meaning="A vehicle promotion applied to a device config. This is a derived measure from context used to build the device configuration.",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalytics.DIM_POPULATIONS),
        AnyUpstream(ProductAnalyticsStaging.STG_VEHICLE_PROMOTIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=3,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalytics.AGG_MMYEF_PROMOTIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_mmyef_promotions(
    context: AssetExecutionContext,
) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]

    return QUERY.format(
        date_start=partition_keys[0],
        date_end=partition_keys[-1],
        product_analytics=get_databases()[Database.PRODUCT_ANALYTICS],
        product_analytics_staging=get_databases()[Database.PRODUCT_ANALYTICS_STAGING],
        autodetect_result_consistency_days=3,
        minimum_consistency_threshold=0.95,
    )
