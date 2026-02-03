from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, Definitions
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """

WITH data AS (
    SELECT
        population.date,
        population.grouping_hash,
        population.grouping_level,
        population.market,
        population.device_type,
        population.equipment_type,
        population.product_name,
        population.variant_name,
        population.cable_name,
        population.make,
        population.model,
        population.year,
        population.engine_type,
        population.fuel_type,
        population.engine_model,
        population.count_distinct_device_id,
        COALESCE(rank.rank, 99) AS product_rank,
        COUNT_IF(COALESCE(coverage.percent_coverage > .8, FALSE)) / COUNT(priority.*) AS coverage

    FROM {product_analytics_staging}.agg_telematics_populations AS population
    JOIN definitions.telematics_market_priority AS priority USING (market, engine_type)
    JOIN definitions.telematics_product_rank AS rank USING (product_name)
    LEFT JOIN {product_analytics_staging}.agg_telematics_actual_coverage_normalized AS coverage USING (type, date, grouping_hash)

    WHERE population.date BETWEEN "{date_start}" AND "{date_end}"
      -- Devices may be swapped between vehicles leading to misclassifications of cables being installed
      -- correctly for a stale MMY. Make sure we have a large enough group consistent in reporting for an
      -- install to be considered.
      AND population.count_distinct_device_id >= 10
      AND population.grouping_level = "market + device_type + product_name + variant_name + cable_name + engine_type + fuel_type + make + model + year + engine_model"
      AND population.device_type = "VG"
    
    GROUP BY ALL
),

recommendations AS (
    SELECT
        date,
        grouping_hash,
        coverage,

        -- Higher ranked products are better, higher average scores are better
        -- For overall ranking, use device counts as a tie breaker. If 
        DENSE_RANK() OVER (
            PARTITION BY
                date, market, device_type, equipment_type, make, model, year, engine_type, fuel_type, engine_model
            ORDER BY
                product_rank DESC,
                coverage DESC,
                count_distinct_device_id DESC
        ) AS suggestion_rank,
        
        DENSE_RANK() OVER (
            PARTITION BY
                date, market, device_type, equipment_type, make, model, year, engine_type, fuel_type, engine_model
            ORDER BY
                coverage DESC
        ) AS supported_rank,
        
        CASE
            WHEN supported_rank = 1 AND suggestion_rank = 1 THEN 'PREFERRED'
            WHEN supported_rank = 1 THEN 'SUPPORTED'
            WHEN suggestion_rank = 1 THEN 'SUGGESTED'
        END AS install_label
    FROM data
    WHERE coverage > 0
)

SELECT * FROM recommendations
WHERE install_label IS NOT NULL
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.GROUPING_HASH,
    Column(
        name="coverage",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="The rank of the product."),
    ),
    Column(
        name="suggestion_rank",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="The rank of the product."),
    ),
    Column(
        name="supported_rank",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="The rank of the product."),
    ),
    Column(
        name="install_label",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The label of the product."),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics MMYEF gaps.",
        row_meaning="Telematics MMYEF gaps for a device over a given time period. Gives the most common missing signal set and the number of devices affected by it.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Definitions.TELEMATICS_PRODUCT_RANK),
        AnyUpstream(Definitions.TELEMATICS_MARKET_PRIORITY),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_ACTUAL_COVERAGE_NORMALIZED),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_POPULATIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_PREFERRED_INSTALL.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_preferred_install(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)