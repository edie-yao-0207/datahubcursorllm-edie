"""
Telematics Outliers Detection Asset

This asset detects outliers in telematics data by comparing values and deltas against
cutoffs derived from operational data. It flags both value outliers and delta outliers
based on statistical thresholds from diagnostic cutoffs.
"""

from pyspark.sql import DataFrame
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    InstanceType,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    Metadata,
    DataType,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
    create_run_config_overrides,
)
from dataweb.assets.datasets.product_analytics.agg_device_stats_primary import (
    METRIC_KEYS,
    PARTITIONS,
)
from dataweb.userpkgs.utils import (
    get_partition_ranges_from_context,
    run_partitioned_queries,
)


QUERY = """
WITH cutoffs_filtered AS (
    SELECT
        date,
        type,
        grouping_hash,
        population_id,
        value_max_adjusted,
        value_min_adjusted,
        delta_max_adjusted,
        delta_min_adjusted
    FROM {product_analytics_staging}.fct_telematics_data_quality_cutoffs
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
      AND type = '{type}'
),



quantiles_data AS (
    SELECT
        date,
        type,
        org_id,
        device_id,
        percentiles_val,
        percentiles_delta,
        min_val,
        max_val,
        delta_min,
        delta_max,

        -- Add P0 and P100 to percentile map
        ARRAY_UNION(
            ARRAY(
                STRUCT(0 AS percentile, min_val AS value_percentile, delta_min AS delta_percentile)
            ),
            ARRAY_UNION(
                TRANSFORM(SEQUENCE(0, 98), i -> STRUCT(
                    i + 1 AS percentile,
                    percentiles_val[i] AS value_percentile,
                    percentiles_delta[i] AS delta_percentile
                )),
                ARRAY(
                    STRUCT(100 AS percentile, max_val AS value_percentile, delta_max AS delta_percentile)
                )
            )
        ) AS percentile_map
    FROM {product_analytics_staging}.agg_device_stats_quantiles
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
      AND type = '{type}'
),

-- Use the pre-computed device-to-population matches
device_population_matches AS (
    SELECT DISTINCT
        dpm.date,
        dpm.org_id,
        dpm.device_id,
        dpm.population_id,
        cf.grouping_hash
    FROM {product_analytics_staging}.dim_device_population_matches dpm
    JOIN cutoffs_filtered cf USING (date, population_id)
    WHERE dpm.date BETWEEN "{date_start}" AND "{date_end}"
),

-- Pre-filter to only rows that could be outliers (based on min/max bounds) AND belong to populations
potential_outliers AS (
    SELECT
        q.*,
        dpm.grouping_hash,
        dpm.population_id,
        cf.value_min_adjusted,
        cf.value_max_adjusted,
        cf.delta_min_adjusted,
        cf.delta_max_adjusted
    FROM quantiles_data q
    JOIN device_population_matches dpm USING (date, org_id, device_id)
    JOIN cutoffs_filtered cf USING (date, type, population_id)
    WHERE q.max_val > cf.value_max_adjusted
       OR q.min_val < cf.value_min_adjusted
       OR q.delta_max > cf.delta_max_adjusted
       OR q.delta_min < cf.delta_min_adjusted
),

-- Now apply percentile filtering only on those rows
percentile_matches AS (
    SELECT
        date,
        type,
        org_id,
        device_id,
        grouping_hash,
        population_id,

        -- Lower bound (value)
        ARRAY_SORT(
            FILTER(percentile_map, x -> x.value_percentile >= value_min_adjusted)
        )[0] AS value_lower_struct,

        -- Upper bound (value)
        ARRAY_SORT(
            FILTER(percentile_map, x -> x.value_percentile <= value_max_adjusted)
        )[SIZE(FILTER(percentile_map, x -> x.value_percentile <= value_max_adjusted)) - 1] AS value_upper_struct,

        -- Lower bound (delta)
        ARRAY_SORT(
            FILTER(percentile_map, x -> x.delta_percentile >= delta_min_adjusted)
        )[0] AS delta_lower_struct,

        -- Upper bound (delta)
        ARRAY_SORT(
            FILTER(percentile_map, x -> x.delta_percentile <= delta_max_adjusted)
        )[SIZE(FILTER(percentile_map, x -> x.delta_percentile <= delta_max_adjusted)) - 1] AS delta_upper_struct,

        value_min_adjusted,
        value_max_adjusted,
        delta_min_adjusted,
        delta_max_adjusted

    FROM potential_outliers
)

SELECT
    date,
    type,
    org_id,
    device_id,
    grouping_hash,
    population_id,

    COALESCE(value_lower_struct.percentile, 100) AS value_lower_percentile,
    COALESCE(value_lower_struct.value_percentile, value_upper_struct.value_percentile) AS value_lower_percentile_value,

    COALESCE(value_upper_struct.percentile, 0) AS value_upper_percentile,
    COALESCE(value_upper_struct.value_percentile, value_lower_struct.value_percentile) AS value_upper_percentile_value,

    COALESCE(delta_lower_struct.percentile, 100) AS delta_lower_percentile,
    COALESCE(delta_lower_struct.delta_percentile, delta_upper_struct.delta_percentile) AS delta_lower_percentile_value,

    COALESCE(delta_upper_struct.percentile, 0) AS delta_upper_percentile,
    COALESCE(delta_upper_struct.delta_percentile, delta_lower_struct.delta_percentile) AS delta_upper_percentile_value,

    value_min_adjusted,
    value_max_adjusted,
    delta_min_adjusted,
    delta_max_adjusted

FROM percentile_matches
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.TYPE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.GROUPING_HASH,
    ColumnType.POPULATION_ID,
    Column(
        name="value_lower_percentile",
        type=DataType.INTEGER,
        metadata=Metadata(
            comment="Lowest percentile that doesn't meet lower bound criteria (0-100)"
        ),
        nullable=True,
    ),
    Column(
        name="value_lower_percentile_value",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Value at the lower percentile threshold"),
        nullable=True,
    ),
    Column(
        name="value_upper_percentile",
        type=DataType.INTEGER,
        metadata=Metadata(
            comment="Lowest percentile that doesn't meet upper bound criteria (0-100)"
        ),
        nullable=True,
    ),
    Column(
        name="value_upper_percentile_value",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Value at the upper percentile threshold"),
        nullable=True,
    ),
    Column(
        name="delta_lower_percentile",
        type=DataType.INTEGER,
        metadata=Metadata(
            comment="Lowest percentile that doesn't meet delta lower bound criteria (0-100)"
        ),
        nullable=True,
    ),
    Column(
        name="delta_lower_percentile_value",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Delta value at the lower percentile threshold"),
        nullable=True,
    ),
    Column(
        name="delta_upper_percentile",
        type=DataType.INTEGER,
        metadata=Metadata(
            comment="Lowest percentile that doesn't meet delta upper bound criteria (0-100)"
        ),
        nullable=True,
    ),
    Column(
        name="delta_upper_percentile_value",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Delta value at the upper percentile threshold"),
        nullable=True,
    ),
    Column(
        name="value_min_adjusted",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Lower bound for value"),
        nullable=True,
    ),
    Column(
        name="value_max_adjusted",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Upper bound for value"),
        nullable=True,
    ),
    Column(
        name="delta_min_adjusted",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Lower bound for delta"),
        nullable=True,
    ),
    Column(
        name="delta_max_adjusted",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Upper bound for delta"),
        nullable=True,
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Detects outliers in telematics data using statistical cutoffs for anomaly detection",
        row_meaning="Each row represents a data point with outlier flags for values and deltas based on statistical thresholds",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=PARTITIONS,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_DATA_QUALITY_CUTOFFS),
        AnyUpstream(ProductAnalyticsStaging.AGG_DEVICE_STATS_QUANTILES),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_POPULATION_MATCHES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_DATA_QUALITY_OUTLIERS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
    ),
)
def fct_telematics_data_quality_outliers(context: AssetExecutionContext) -> DataFrame:
    partitions = get_partition_ranges_from_context(
        context, full_secondary_keys=METRIC_KEYS
    )

    def build_query(metric_type: str) -> str:
        return format_date_partition_query(
            QUERY,
            context,
            partitions=partitions,
            type=metric_type,
        )

    return run_partitioned_queries(
        context=context,
        query_builder=build_query,
        secondary_keys=partitions.selected_secondary_keys,
        repartition_cols=[str(ColumnType.DATE), str(ColumnType.TYPE)],
    )
