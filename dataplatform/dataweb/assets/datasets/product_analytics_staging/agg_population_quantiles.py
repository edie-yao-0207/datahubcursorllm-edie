"""
Population Quantiles Aggregation

This asset aggregates device-level quantile statistics up to population-level summaries,
enabling analysis of fleet behavior patterns across different device populations. It serves
as the foundational layer for population-based analytics by consolidating individual device
metrics into meaningful group statistics.

## Purpose
- **Population Analytics**: Transform device-level quantiles into population-level distributions
  for fleet health monitoring and comparative analysis
- **Statistical Aggregation**: Properly combine quantiles, means, and standard deviations across
  devices while maintaining statistical validity
- **Multi-Dimensional Grouping**: Support flexible population definitions through both
  grouping_hash (coverage-aligned) and population_id (legacy) approaches
- **Dual Metric Support**: Handle both absolute value metrics and period-over-period delta metrics

## Processing Logic
1. **Base Data Join**: Combines device quantiles with device dimensions to enable population grouping
2. **Quantile Explosion**: Unpacks device-level quantile arrays into individual data points
   for proper population-level re-aggregation
3. **Population Grouping**: Groups devices by population dimensions using configurable grouping sets
4. **Statistical Re-aggregation**: 
   - Computes population quantiles using PERCENTILE_APPROX across exploded device quantiles
   - Calculates weighted population means and standard deviations using proper variance formulas
   - Maintains separate statistics for value and delta metrics
5. **Result Reconstruction**: Rebuilds quantile arrays and summary statistics at population level

## Input Data
- **Primary Source**: `agg_device_stats_quantiles` - Device-level quantile summaries with percentile arrays
- **Dimensions**: `dim_device_dimensions` - Device attributes for population grouping
- **Metrics**: Both value metrics (absolute measurements) and delta metrics (changes over time)

## Output Data
Each row represents statistical summaries for a device population on a specific date:
- **Population Identifiers**: type, grouping_hash, population_id for flexible grouping strategies
- **Quantile Arrays**: value_quantiles and delta_quantiles with population-level percentiles
- **Summary Statistics**: Mean, standard deviation, device count, and sample size for both metrics
- **Statistical Validity**: Proper variance calculations that account for within-device and between-device variation

## Key Features
- **Dual Grouping Strategy**: Supports both grouping_hash (modern, coverage-aligned) and 
  population_id (legacy) for pipeline migration flexibility
- **Proper Statistical Aggregation**: Uses weighted means and correct variance formulas when
  combining device-level statistics into population summaries
- **High Precision Quantiles**: Uses PERCENTILE_APPROX with 10,000 buckets for accurate
  population-level percentile estimation
- **Memory Efficiency**: Processes data in partitioned queries to handle large device populations

## Use Cases
- **Fleet Health Monitoring**: Population-level dashboards showing device performance distributions
- **Comparative Analysis**: Cross-population comparisons (e.g., vehicle types, regions, customer segments)
- **Anomaly Detection**: Identify populations with unusual statistical patterns
- **Capacity Planning**: Population-based resource utilization and scaling decisions
- **Performance Benchmarking**: Establish population baselines for individual device assessment
"""

from dagster import AssetExecutionContext
from dataweb import (
    table,
    build_general_dq_checks,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    Database,
    DQCheckMode,
    TableType,
    WarehouseWriteMode,
    FRESHNESS_SLO_9AM_PST,
    FIRMWAREVDP,
    InstanceType,
)
from dataweb.userpkgs.firmware.constants import (
    ConcurrencyKey,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    Metadata,
    DataType,
    columns_to_schema,
    array_of,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_partition_ranges_from_context,
    run_partitioned_queries,
)
from dataweb.userpkgs.query import create_run_config_overrides
from pyspark.sql import DataFrame
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, ProductAnalytics
from dataweb.assets.datasets.product_analytics_staging.agg_device_stats_quantiles import (
    PARTITIONS,
    QUANTILES,
)
from dataweb.assets.datasets.product_analytics.agg_device_stats_primary import (
    METRIC_KEYS,
)
from dataweb.userpkgs.query import (
    format_agg_date_partition_query,
)
from dataweb.userpkgs.firmware.populations import (
    DIMENSIONS,
    GROUPINGS,
)

COLUMNS = [
    ColumnType.DATE,
    ColumnType.TYPE,
    # Both grouping hash and population id are used to group into populations. The intention is to move our
    # pipelines to use grouping hash instead of population id so we can align all of our aggregates. Coverage
    # pipelines currently use grouping hash. The grouping sets for both of these are different.
    ColumnType.GROUPING_HASH,
    ColumnType.POPULATION_ID,
    Column(name="value_quantiles", type=array_of(DataType.DOUBLE), metadata=Metadata(comment="Quantiles of the value metric."), nullable=True),
    Column(name="delta_quantiles", type=array_of(DataType.DOUBLE), metadata=Metadata(comment="Quantiles of the delta metric."), nullable=True),
    Column(name="value_device_count", type=DataType.LONG, metadata=Metadata(comment="Number of devices with a value metric."), nullable=True),
    Column(name="value_sample_size", type=DataType.LONG, metadata=Metadata(comment="Number of samples with a value metric."), nullable=True),
    Column(name="value_mean", type=DataType.DOUBLE, metadata=Metadata(comment="Mean of the value metric."), nullable=True),
    Column(name="value_std", type=DataType.DOUBLE, metadata=Metadata(comment="Standard deviation of the value metric."), nullable=True),
    Column(name="delta_device_count", type=DataType.LONG, metadata=Metadata(comment="Number of devices with a delta metric."), nullable=True),
    Column(name="delta_sample_size", type=DataType.LONG, metadata=Metadata(comment="Number of samples with a delta metric."), nullable=True),
    Column(name="delta_mean", type=DataType.DOUBLE, metadata=Metadata(comment="Mean of the delta metric."), nullable=True),
    Column(name="delta_std", type=DataType.DOUBLE, metadata=Metadata(comment="Standard deviation of the delta metric."), nullable=True),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
WITH base AS (
    SELECT
        q.date,
        q.org_id,
        q.device_id,
        q.type,
        {dimensions},
        q.count AS sample_size,
        q.mean AS value_mean,
        q.stddev AS value_std,
        q.delta_mean,
        q.delta_std,
        q.percentiles_val,
        q.percentiles_delta
    FROM {product_analytics_staging}.agg_device_stats_quantiles q
    JOIN product_analytics.dim_device_dimensions d
        USING (date, org_id, device_id)
    WHERE q.date BETWEEN '{date_start}' AND '{date_end}'
      AND q.type = '{type}'
),

exploded AS (
    SELECT
        date,
        type,
        {dimensions},
        'value' AS quantile_type,
        sampled_value,
        sample_size,
        value_mean,
        value_std,
        delta_mean,
        delta_std
    FROM base
    LATERAL VIEW POSEXPLODE(percentiles_val) pq AS quantile_index, sampled_value

    UNION ALL

    SELECT
        date,
        type,
        {dimensions},
        'delta' AS quantile_type,
        sampled_value,
        sample_size,
        value_mean,
        value_std,
        delta_mean,
        delta_std
    FROM base
    LATERAL VIEW POSEXPLODE(percentiles_delta) pq AS quantile_index, sampled_value
),

aggregated AS (
    SELECT
        date,
        type,
        {dimensions},
        quantile_type,
        {grouping_hash},
        {population_id},

        COUNT(*) AS device_count,
        SUM(sample_size) AS total_sample_size,

        -- Value stats
        SUM(sample_size * value_mean) AS weighted_sum_val,
        SUM(sample_size * POW(value_std, 2)) AS within_var_val,
        SUM(sample_size * POW(value_mean, 2)) AS mean_sq_val,

        -- Delta stats
        SUM(sample_size * delta_mean) AS weighted_sum_delta,
        SUM(sample_size * POW(delta_std, 2)) AS within_var_delta,
        SUM(sample_size * POW(delta_mean, 2)) AS mean_sq_delta,

        -- Quantiles
        PERCENTILE_APPROX(sampled_value, ARRAY({quantiles}), 10000) AS group_quantiles

    FROM exploded
    GROUP BY
        date,
        type,
        quantile_type,
        {grouping_sets}
),

final_stats AS (
    SELECT
        date,
        type,
        grouping_hash,
        population_id,
        quantile_type,
        device_count,
        total_sample_size,

        -- Value
        weighted_sum_val / total_sample_size AS value_mean,
        SQRT(
            (within_var_val + mean_sq_val - POW(weighted_sum_val, 2) / total_sample_size)
            / total_sample_size
        ) AS value_std,

        -- Delta
        weighted_sum_delta / total_sample_size AS delta_mean,
        SQRT(
            (within_var_delta + mean_sq_delta - POW(weighted_sum_delta, 2) / total_sample_size)
            / total_sample_size
        ) AS delta_std,

        group_quantiles
    FROM aggregated
),

value_stats AS (
    SELECT
        date,
        type,
        grouping_hash,
        population_id,
        device_count AS value_device_count,
        total_sample_size AS value_sample_size,
        weighted_sum_val / total_sample_size AS value_mean,
        SQRT(
            (within_var_val + mean_sq_val - POW(weighted_sum_val, 2) / total_sample_size)
            / total_sample_size
        ) AS value_std,
        group_quantiles AS value_quantiles
    FROM aggregated
    WHERE quantile_type = 'value'
),

delta_stats AS (
    SELECT
        date,
        type,
        grouping_hash,
        population_id,
        device_count AS delta_device_count,
        total_sample_size AS delta_sample_size,
        weighted_sum_delta / total_sample_size AS delta_mean,
        SQRT(
            (within_var_delta + mean_sq_delta - POW(weighted_sum_delta, 2) / total_sample_size)
            / total_sample_size
        ) AS delta_std,
        group_quantiles AS delta_quantiles
    FROM aggregated
    WHERE quantile_type = 'delta'
)

SELECT
    v.date,
    v.type,
    v.grouping_hash,
    v.population_id,
    v.value_quantiles,
    d.delta_quantiles,
    v.value_device_count,
    v.value_sample_size,
    v.value_mean,
    v.value_std,
    d.delta_device_count,
    d.delta_sample_size,
    d.delta_mean,
    d.delta_std
FROM value_stats v
JOIN delta_stats d
USING (date, type, grouping_hash, population_id)
"""

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Aggregates device-level quantile statistics into population-level summaries using proper statistical methods for fleet behavior analysis. "
        "Transforms individual device metrics into meaningful population distributions by exploding device quantile arrays, grouping by population dimensions, "
        "and re-aggregating using weighted means and variance calculations. Supports flexible population definitions through both grouping_hash (coverage-aligned) "
        "and population_id (legacy) approaches, with separate handling of value metrics (absolute measurements) and delta metrics (period-over-period changes). "
        "Serves as the foundational layer for population-based analytics, comparative analysis, and fleet health monitoring.",
        row_meaning="Population-level quantile statistics and summary metrics for a device population on a specific date, with proper statistical aggregation from constituent devices",
        related_table_info={},
        table_type=TableType.MONTHLY_REPORTING_AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=PARTITIONS,
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    concurrency_key=ConcurrencyKey.VDP,
    single_run_backfill=True,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_DEVICE_STATS_QUANTILES),
        AnyUpstream(ProductAnalytics.DIM_DEVICE_DIMENSIONS),
    ],
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_POPULATION_QUANTILES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    run_config_overrides=create_run_config_overrides(
        min_workers=1,
        max_workers=32,
        driver_instance_type=InstanceType.MD_FLEET_4XLARGE,
        worker_instance_type=InstanceType.MD_FLEET_4XLARGE,
    ),
    max_retries=5,
)
def agg_population_quantiles(context: AssetExecutionContext) -> DataFrame:
    partitions = get_partition_ranges_from_context(context, full_secondary_keys=METRIC_KEYS)

    def build_query(metric_type: str) -> str:
        query_template = format_agg_date_partition_query(
            context,
            QUERY,
            DIMENSIONS,
            GROUPINGS,
        )
        return partitions.format_query(
            query_template,
            type=metric_type,
            quantiles=",".join(map(str, QUANTILES)),
        )

    return run_partitioned_queries(
        context=context,
        query_builder=build_query,
        secondary_keys=partitions.selected_secondary_keys,
        repartition_cols=[str(ColumnType.DATE), str(ColumnType.TYPE)],
    )