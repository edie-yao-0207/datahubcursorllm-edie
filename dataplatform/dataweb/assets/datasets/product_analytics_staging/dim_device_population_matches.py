"""
Device Population Matches Dimension Asset

This asset creates a generalized mapping between devices and all populations they belong to
based on dimension matching. It processes each population grouping separately and combines
the results, providing an efficient lookup table for downstream analytics.
"""

from pyspark.sql import DataFrame
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataclasses import replace
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
    ColumnType,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.table import ProductAnalytics, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.populations import Population
from dataweb.userpkgs.firmware.schema import columns_to_names
from dataweb.userpkgs.utils import partition_key_ranges_from_context
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)

# Schema and primary keys

# Schema definition
COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    replace(ColumnType.POPULATION_ID.value, primary_key=True),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

# Base query template for each population type
POPULATION_MATCH_QUERY = """
WITH populations AS (
    SELECT
        date,
        population_id,
        {population_dimensions}
    FROM product_analytics.dim_populations
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
      AND population_type = '{population_type}'
),

device_dimensions AS (
    SELECT
        date,
        org_id,
        device_id,
        {population_dimensions}
    FROM product_analytics.dim_device_dimensions
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
),

device_population_matches AS (
    SELECT DISTINCT
        dd.date,
        dd.org_id,
        dd.device_id,
        p.population_id
    FROM device_dimensions dd
    JOIN populations p
        ON p.date = dd.date
        AND {population_join_conditions}
)

SELECT * FROM device_population_matches
"""


def build_population_join_conditions(population_dimensions: list[str]) -> str:
    """Build join conditions for the specific population dimensions."""
    if not population_dimensions:
        # For empty populations (like ALL), match all devices with no additional conditions
        return "1=1"

    return " AND ".join([f"dd.{col} <=> p.{col}" for col in population_dimensions])


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Generalized mapping between devices and all populations they belong to based on dimension matching",
        row_meaning="Each row represents a device that belongs to a specific population based on shared characteristics",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalytics.DIM_POPULATIONS),
        AnyUpstream(ProductAnalytics.DIM_DEVICE_DIMENSIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_DEVICE_POPULATION_MATCHES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_device_population_matches(context: AssetExecutionContext) -> DataFrame:
    """
    Create device-to-population matches by processing each population grouping separately.

    Uses the GROUPINGS list from populations.py to efficiently process each population
    type without querying for distinct values. Each grouping is processed separately
    and results are combined.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Get partition date range from context
    partition_keys = partition_key_ranges_from_context(context)[0]
    date_start, date_end = partition_keys[0], partition_keys[-1]

    context.log.info(
        f"Processing device population matches for date range: {date_start} to {date_end}"
    )

    # Use Population enum instances to get all population types and their dimensions
    populations_info = []
    for population in Population:
        population_type = str(population)
        population_dimensions = (
            columns_to_names(*population.value) if population.value else []
        )
        populations_info.append((population_type, population_dimensions))

    context.log.info(
        f"Processing {len(populations_info)} population groupings from Population enum"
    )

    # Process each population type separately and combine results
    combined_dfs = []

    for i, (pop_type, pop_dimensions) in enumerate(populations_info):
        context.log.info(
            f"Processing grouping {i+1}/{len(populations_info)}: {pop_type} with dimensions: {pop_dimensions}"
        )

        if pop_dimensions:
            population_dimensions_sql = ", ".join(pop_dimensions)
        else:
            population_dimensions_sql = "CAST(NULL AS STRING) AS dummy_col"

        formatted_query = format_date_partition_query(
            POPULATION_MATCH_QUERY,
            context,
            population_type=pop_type,
            population_dimensions=population_dimensions_sql,
            population_join_conditions=build_population_join_conditions(pop_dimensions),
        )

        context.log.info(f"{formatted_query}")

        pop_df = spark.sql(formatted_query)
        combined_dfs.append(pop_df)

        context.log.info(f"Completed grouping: {pop_type}")

    # Union all DataFrames
    if not combined_dfs:
        context.log.warning("No population groupings found, returning empty DataFrame")
        return spark.createDataFrame([], SCHEMA)

    # Start with first DataFrame and union the rest
    result_df = combined_dfs[0]
    for df in combined_dfs[1:]:
        result_df = result_df.union(df)

    result_df = result_df.repartition("date")

    context.log.info(
        f"Successfully processed all {len(populations_info)} population groupings, returning combined results"
    )

    return result_df
