from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.query import (
    format_date_partition_query,
)
from dataweb.assets.datasets.product_analytics.agg_device_stats_primary import (
    METRICS,
    MAPPING,
    METRIC_KEYS,
    PARTITIONS,
)
from dataweb.userpkgs.extract_utils import select_extract_pattern
from dataweb.userpkgs.firmware.metric import Metric
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    Database,
    DQCheckMode,
    InstanceType,
    TableType,
    WarehouseWriteMode,
    FRESHNESS_SLO_9AM_PST,
    FIRMWAREVDP,
)
from dataweb.userpkgs.firmware.constants import (
    ConcurrencyKey,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.schema import (
    Column,
    Metadata,
    ColumnType,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    array_of,
    DataType,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_partition_ranges_from_context,
    run_partitioned_queries,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from pyspark.sql import DataFrame

# Schema
COLUMNS = [
    ColumnType.DATE,
    ColumnType.TYPE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(name="count", type=DataType.LONG, nullable=False, primary_key=False, metadata=Metadata(comment="The number of observations in the group.")),
    Column(name="mean", type=DataType.DOUBLE, nullable=True, primary_key=False, metadata=Metadata(comment="The mean value of the group.")),
    Column(name="stddev", type=DataType.DOUBLE, nullable=True, primary_key=False, metadata=Metadata(comment="The standard deviation of the group.")),
    Column(name="min_val", type=DataType.DOUBLE, nullable=True, primary_key=False, metadata=Metadata(comment="Minimum value in the group.")),
    Column(name="max_val", type=DataType.DOUBLE, nullable=True, primary_key=False, metadata=Metadata(comment="Maximum value in the group.")),
    Column(name="percentiles_val", type=array_of(DataType.DOUBLE), nullable=True, primary_key=False, metadata=Metadata(comment="The percentiles of the value.")),
    Column(name="percentiles_delta", type=array_of(DataType.DOUBLE), nullable=True, primary_key=False, metadata=Metadata(comment="The percentiles of the delta.")),
    Column(name="delta_mean", type=DataType.DOUBLE, nullable=True, primary_key=False, metadata=Metadata(comment="The mean of the delta.")),
    Column(name="delta_std", type=DataType.DOUBLE, nullable=True, primary_key=False, metadata=Metadata(comment="The standard deviation of the delta.")),
    Column(name="delta_min", type=DataType.DOUBLE, nullable=True, primary_key=False, metadata=Metadata(comment="The minimum value of the delta.")),
    Column(name="delta_max", type=DataType.DOUBLE, nullable=True, primary_key=False, metadata=Metadata(comment="The maximum value of the delta.")),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

# Quantile array
QUANTILES = tuple(x / 100 for x in range(1, 100))

GROUPED_TEMPLATE = """
WITH base AS (
    {extract}
),

with_prev AS (
    SELECT
        date,
        org_id,
        device_id,
        value,
        time,
        LAG(value) OVER (PARTITION BY org_id, device_id ORDER BY time) AS prev_value,
        LAG(time)  OVER (PARTITION BY org_id, device_id ORDER BY time) AS prev_time
    FROM base
),

grouped AS (
    SELECT
        date,
        '{type}' AS type,
        org_id,
        device_id,
        COUNT(*) AS count,
        AVG(value) AS mean,
        STDDEV(value) AS stddev,
        MIN(value) AS min_val,
        MAX(value) AS max_val,
        PERCENTILE_APPROX(value, ARRAY({quantiles}), 10000) AS value_quantiles,

        -- Delta stats
        PERCENTILE_APPROX(
            (value - prev_value) / (time - prev_time),
            ARRAY({quantiles}),
            10000
        ) AS delta_quantiles,
        AVG((value - prev_value) / (time - prev_time)) AS delta_mean,
        STDDEV((value - prev_value) / (time - prev_time)) AS delta_std,
        MIN((value - prev_value) / (time - prev_time)) AS delta_min,
        MAX((value - prev_value) / (time - prev_time)) AS delta_max

    FROM with_prev
    WHERE prev_value IS NOT NULL AND (time - prev_time) > 0
    GROUP BY date, org_id, device_id
)

SELECT
    date,
    type,
    org_id,
    device_id,
    count,
    mean,
    stddev,
    min_val,
    max_val,
    value_quantiles AS percentiles_val,
    delta_quantiles AS percentiles_delta,
    delta_mean,
    delta_std,
    delta_min,
    delta_max
FROM grouped
"""



@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Compute quantile-based summary statistics for diagnostic quality analysis signals across metrics per day.",
        row_meaning="Quantile-based summary statistics for a given device",
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
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    upstreams=[AnyUpstream(metric.type) for metric in METRICS],
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_DEVICE_STATS_QUANTILES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    max_retries=5,
    run_config_overrides=create_run_config_overrides(
        min_workers=1,
        max_workers=32,
        driver_instance_type=InstanceType.MD_FLEET_4XLARGE,
        worker_instance_type=InstanceType.MD_FLEET_4XLARGE,
    ),
)
def agg_device_stats_quantiles(context: AssetExecutionContext) -> DataFrame:
    partitions = get_partition_ranges_from_context(context, full_secondary_keys=METRIC_KEYS)

    def build_query(metric_type: str) -> str:
        # Get the metric and select appropriate extract pattern
        metric = MAPPING[metric_type]

        extract = select_extract_pattern(
            metric=metric,
            date_start=partitions.date_start,
            date_end=partitions.date_end,
        )

        formatted_query = format_date_partition_query(
            GROUPED_TEMPLATE,
            context,
            partitions=partitions,
            type=metric_type,
            extract=extract,
            quantiles=",".join(map(str, QUANTILES)),
        )

        return formatted_query

    return run_partitioned_queries(
        context=context,
        query_builder=build_query,
        secondary_keys=partitions.selected_secondary_keys,
        repartition_cols=[str(ColumnType.DATE), str(ColumnType.TYPE)],
    )