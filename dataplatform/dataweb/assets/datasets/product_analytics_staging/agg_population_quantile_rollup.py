"""
Population Quantile Rollup

This asset creates a rolling 14-day aggregation of population-level quantiles, providing
smoothed statistical summaries that reduce day-to-day noise in device metrics. The rolling
window approach helps identify longer-term trends while maintaining statistical robustness
for downstream analysis and alerting.

## Purpose
- **Temporal Smoothing**: Reduces daily volatility in quantile measurements by computing
  rolling statistics over a 14-day window
- **Trend Analysis**: Enables detection of gradual changes in device population behavior
  that might be masked by daily fluctuations
- **Statistical Robustness**: Provides more stable quantile estimates for populations
  with varying daily sample sizes

## Processing Logic
1. **Rolling Window**: For each target date, collects quantile data from the past 14 days
   (current date + 13 prior days)
2. **Quantile Explosion**: Unpacks existing quantile arrays (value_quantiles, delta_quantiles)
   into individual data points across the rolling window
3. **Re-aggregation**: Computes new quantiles using PERCENTILE_APPROX across the exploded
   rolling window data
4. **Reconstruction**: Rebuilds value_quantiles and delta_quantiles arrays with the
   rolling statistics

## Input Data
- **Source**: `agg_population_quantiles` - Daily population-level quantile summaries
- **Lookback**: 14-day rolling window (current date + 13 historical days)
- **Metrics**: Both value metrics (absolute measurements) and delta metrics (period-over-period changes)

## Output Data
Each row represents rolling quantile statistics for a population on a specific date:
- **Population Dimensions**: type, grouping_hash, population_id
- **Quantile Arrays**: value_quantiles and delta_quantiles with smoothed statistics
- **Time Series**: Daily granularity with 14-day rolling calculations

## Use Cases
- **Anomaly Detection**: More stable baselines for identifying unusual population behavior
- **Performance Monitoring**: Trend analysis for device fleet health metrics
- **Capacity Planning**: Smoothed resource utilization projections
- **Reporting**: Less noisy population statistics for executive dashboards
"""

from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
    FRESHNESS_SLO_9AM_PST,
    FIRMWAREVDP,
    InstanceType,
)
from dataweb.userpkgs.firmware.constants import ConcurrencyKey
from dataweb.userpkgs.query import (
    format_date_partition_query,
    create_run_config_overrides,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    array_of,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.utils import (
    build_table_description,
    get_partition_ranges_from_context,
    run_partitioned_queries,
)
from dataweb.assets.datasets.product_analytics_staging.agg_device_stats_quantiles import (
    PARTITIONS,
    QUANTILES,
)
from dataweb.assets.datasets.product_analytics.agg_device_stats_primary import METRIC_KEYS
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from pyspark.sql import DataFrame

COLUMNS = [
    ColumnType.DATE,
    ColumnType.TYPE,
    ColumnType.GROUPING_HASH,
    ColumnType.POPULATION_ID,
    Column(name="value_quantiles", type=array_of(DataType.DOUBLE), metadata=Metadata(comment="Quantiles of the value metric."), nullable=True),
    Column(name="delta_quantiles", type=array_of(DataType.DOUBLE), metadata=Metadata(comment="Quantiles of the delta metric."), nullable=True),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

TEMPLATE_QUERY = """
WITH base AS (
    SELECT *
    FROM {product_analytics_staging}.agg_population_quantiles
    WHERE date BETWEEN DATE_SUB('{date_start}', 13) AND '{date_end}'
      AND type = '{type}'
),

dates AS (
    SELECT DISTINCT date
    FROM base
    WHERE date BETWEEN '{date_start}' AND '{date_end}'
),

rolling_base AS (
    SELECT
        d.date AS target_date,
        b.date,
        b.grouping_hash,
        b.population_id,
        b.type,
        b.value_quantiles,
        b.delta_quantiles,
        b.value_sample_size
    FROM dates d
    JOIN base b
      ON b.date BETWEEN DATE_SUB(d.date, 13) AND d.date
),

exploded AS (
    SELECT
        target_date AS date,
        type,
        grouping_hash,
        population_id,
        sampled_value,
        quantile_type
    FROM (
        SELECT *, posexplode(value_quantiles) AS (quantile_index, sampled_value), 'value' AS quantile_type FROM rolling_base
        UNION ALL
        SELECT *, posexplode(delta_quantiles) AS (quantile_index, sampled_value), 'delta' AS quantile_type FROM rolling_base
    )
),

agg AS (
    SELECT
        date,
        type,
        grouping_hash,
        population_id,
        quantile_type,
        PERCENTILE_APPROX(sampled_value, ARRAY({quantiles}), 10000) AS group_quantiles
    FROM exploded
    GROUP BY ALL
),

value_stats AS (
    SELECT date, type, grouping_hash, population_id, group_quantiles AS value_quantiles
    FROM agg
    WHERE quantile_type = 'value'
),

delta_stats AS (
    SELECT date, type, grouping_hash, population_id, group_quantiles AS delta_quantiles
    FROM agg
    WHERE quantile_type = 'delta'
),

data AS (
    SELECT
        v.date,
        v.type,
        v.grouping_hash,
        v.population_id,
        v.value_quantiles,
        d.delta_quantiles
    FROM value_stats v
    JOIN delta_stats d USING (date, type, grouping_hash, population_id)
)

SELECT *
FROM data
WHERE date BETWEEN '{date_start}' AND '{date_end}'
"""

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Rolling 14-day aggregation of population-level quantiles that provides temporally smoothed statistical summaries for trend analysis and anomaly detection. "
        "Reduces daily volatility in device metrics by computing rolling quantiles across a 14-day window, enabling detection of gradual behavioral changes while maintaining statistical robustness. "
        "Supports both value metrics (absolute measurements) and delta metrics (period-over-period changes) with proper quantile re-aggregation across the rolling window.",
        row_meaning="Rolling 14-day quantile statistics for a device population on a specific date, with smoothed percentile arrays and temporal trend indicators",
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
    concurrency_key=ConcurrencyKey.VDP,
    single_run_backfill=True,
    upstreams=[AnyUpstream(ProductAnalyticsStaging.AGG_POPULATION_QUANTILES)],
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_POPULATION_QUANTILE_ROLLUP.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    max_retries=3,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
    ),
)
def agg_population_quantile_rollup(context: AssetExecutionContext) -> DataFrame:
    partitions = get_partition_ranges_from_context(context, full_secondary_keys=METRIC_KEYS)

    def build_query(metric_type: str) -> str:
        return format_date_partition_query(
            TEMPLATE_QUERY,
            context,
            partitions=partitions,
            type=metric_type,
            quantiles=','.join(map(str, QUANTILES)),
        )

    return run_partitioned_queries(
        context=context,
        query_builder=build_query,
        secondary_keys=partitions.selected_secondary_keys,
        repartition_cols=[str(ColumnType.DATE), str(ColumnType.TYPE)],
    )