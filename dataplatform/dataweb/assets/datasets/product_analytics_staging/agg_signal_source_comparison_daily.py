"""
Daily Signal Source Comparison Metrics

This asset creates a daily device-level aggregation showing source comparison metrics for each OBD signal value.
For devices with multiple sources providing the same OBD signal, it calculates unified metrics that work for
both monotonic signals (odometer) and variable signals (temperature): coefficient of variation (CV), range ratio,
interquartile range (IQR), source agreement score, and max divergence percentage.

Each row represents one device per day with a map from OBD value to struct containing source comparison metrics.
This structure prevents row explosion (one row per device per day instead of one row per device per OBD value)
and provides efficient lookup of all comparison metrics for any signal. Use MAP_KEYS(obd_value_to_comparison_map)
to get all OBD values, or obd_value_to_comparison_map[obd_value] to get the metrics for a specific signal.

Metrics are only calculated when multiple sources exist (source_count > 1). For single-source signals, divergence
metrics are set to NULL to indicate no comparison is possible.
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    map_of,
    struct_with_comments,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import (
    create_run_config_overrides,
    format_date_partition_query,
)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="obd_value_to_comparison_map",
        type=map_of(
            DataType.LONG,  # obd_value as key
            struct_with_comments(
                ("source_count", DataType.LONG, "Number of unique sources (bus_id, ecu_id, request_id, data_id combinations) providing this OBD signal"),
                ("cv", DataType.DOUBLE, "Coefficient of variation (std_dev / mean) across sources. NULL if only one source or mean = 0. Normalized spread metric that works for both monotonic and variable signals."),
                ("range_ratio", DataType.DOUBLE, "Normalized value spread: (max - min) / mean across sources. NULL if only one source or mean = 0."),
                ("iqr", DataType.DOUBLE, "Interquartile range (P75 - P25) across sources. NULL if only one source."),
                ("agreement_score", DataType.DOUBLE, "Percentage of sources within ±5% of median value. NULL if only one source. Higher values indicate better source agreement."),
                ("max_divergence_pct", DataType.DOUBLE, "Maximum difference between any two sources as percentage of mean. NULL if only one source or mean = 0."),
                ("mean_value", DataType.DOUBLE, "Average value across all sources (average of source mean values)"),
                ("median_value", DataType.DOUBLE, "Median value across all sources (median of source median values)"),
                ("min_value", DataType.DOUBLE, "Minimum value across all sources (minimum of source min values)"),
                ("max_value", DataType.DOUBLE, "Maximum value across all sources (maximum of source max values)"),
            ),
            value_contains_null=True,
        ),
        nullable=False,
        metadata=Metadata(
            comment="Map from OBD signal value (obd_value) to struct containing source comparison metrics: source_count, cv (coefficient of variation), range_ratio, iqr (interquartile range), agreement_score, max_divergence_pct, and aggregate value statistics (mean, median, min, max). Use MAP_KEYS(obd_value_to_comparison_map) to get all OBD values, or obd_value_to_comparison_map[obd_value] to get the metrics for a specific signal. This structure prevents row explosion (one row per device per day instead of one row per device per OBD value).",
        ),
    ),
    Column(
        name="total_obd_values",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total number of unique OBD signal values (keys in obd_value_to_comparison_map) for the device on this day.",
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """
WITH source_data AS (
    SELECT
        sc.date,
        sc.org_id,
        sc.device_id,
        obd_value_key AS obd_value,
        source.bus_id,
        source.ecu_id,
        source.request_id,
        source.data_id,
        source.source_stats.mean_value AS source_mean,
        source.source_stats.median_value AS source_median,
        source.source_stats.min_value AS source_min,
        source.source_stats.max_value AS source_max,
        source.source_stats.p25_value AS source_p25,
        source.source_stats.p75_value AS source_p75
    FROM {product_analytics_staging}.agg_signal_cache_sources_by_obd_value sc
    LATERAL VIEW explode(obd_value_to_sources_map) AS obd_value_key, signal_data
    LATERAL VIEW explode(signal_data.sources) AS source
    WHERE sc.date BETWEEN "{date_start}" AND "{date_end}"
        AND signal_data IS NOT NULL
        AND signal_data.sources IS NOT NULL
        AND source.source_stats IS NOT NULL
        AND source.source_stats.mean_value IS NOT NULL
        AND source.source_stats.median_value IS NOT NULL
),

source_stats AS (
    SELECT
        date,
        org_id,
        device_id,
        obd_value,
        COUNT(DISTINCT STRUCT(bus_id, ecu_id, request_id, data_id)) AS source_count,
        -- Aggregate statistics across sources
        AVG(source_mean) AS mean_value,
        PERCENTILE_APPROX(source_median, 0.5) AS median_value,
        MIN(source_min) AS min_value,
        MAX(source_max) AS max_value,
        PERCENTILE_APPROX(source_p25, 0.5) AS p25_value,
        PERCENTILE_APPROX(source_p75, 0.5) AS p75_value,
        -- For CV calculation
        STDDEV_POP(source_mean) AS std_dev_mean
    FROM source_data
    GROUP BY date, org_id, device_id, obd_value
),

source_agreement AS (
    SELECT
        ss.date,
        ss.org_id,
        ss.device_id,
        ss.obd_value,
        ss.source_count,
        ss.mean_value,
        ss.median_value,
        ss.min_value,
        ss.max_value,
        ss.p25_value,
        ss.p75_value,
        ss.std_dev_mean,
        -- Calculate agreement score: percentage of sources within ±5% of median
        CASE
            WHEN ss.source_count > 1 AND ss.median_value != 0 THEN
                CAST(SUM(CASE 
                    WHEN ABS(sd.source_median - ss.median_value) / ABS(ss.median_value) <= 0.05 
                    THEN 1 ELSE 0 
                END) AS DOUBLE) / CAST(ss.source_count AS DOUBLE) * 100.0
            ELSE NULL
        END AS agreement_score
    FROM source_stats ss
    LEFT JOIN source_data sd
        ON ss.date = sd.date
        AND ss.org_id = sd.org_id
        AND ss.device_id = sd.device_id
        AND ss.obd_value = sd.obd_value
    GROUP BY 
        ss.date,
        ss.org_id,
        ss.device_id,
        ss.obd_value,
        ss.source_count,
        ss.mean_value,
        ss.median_value,
        ss.min_value,
        ss.max_value,
        ss.p25_value,
        ss.p75_value,
        ss.std_dev_mean
    ),

comparison_metrics AS (
    SELECT
        date,
        org_id,
        device_id,
        obd_value,
        source_count,
        -- CV: std_dev / mean (NULL if only one source or mean = 0)
        CASE
            WHEN source_count > 1 AND mean_value != 0 AND mean_value IS NOT NULL THEN
                std_dev_mean / ABS(mean_value)
            ELSE NULL
        END AS cv,
        -- Range ratio: (max - min) / mean (NULL if only one source or mean = 0)
        CASE
            WHEN source_count > 1 AND mean_value != 0 AND mean_value IS NOT NULL THEN
                (max_value - min_value) / ABS(mean_value)
            ELSE NULL
        END AS range_ratio,
        -- IQR: P75 - P25 (NULL if only one source)
        CASE
            WHEN source_count > 1 THEN p75_value - p25_value
            ELSE NULL
        END AS iqr,
        agreement_score,
        -- Max divergence: (max - min) / mean as percentage (NULL if only one source or mean = 0)
        CASE
            WHEN source_count > 1 AND mean_value != 0 AND mean_value IS NOT NULL THEN
                ((max_value - min_value) / ABS(mean_value)) * 100.0
            ELSE NULL
        END AS max_divergence_pct,
        mean_value,
        median_value,
        min_value,
        max_value
    FROM source_agreement
),

device_daily_aggregation AS (
    SELECT
        cm.date,
        cm.org_id,
        cm.device_id,
        MAP_FROM_ENTRIES(
            COLLECT_LIST(
                STRUCT(
                    cm.obd_value AS key,
                    STRUCT(
                        cm.source_count,
                        cm.cv,
                        cm.range_ratio,
                        cm.iqr,
                        cm.agreement_score,
                        cm.max_divergence_pct,
                        cm.mean_value,
                        cm.median_value,
                        cm.min_value,
                        cm.max_value
                    ) AS value
                )
            )
        ) AS obd_value_to_comparison_map,
        CAST(COUNT(DISTINCT cm.obd_value) AS BIGINT) AS total_obd_values
    FROM comparison_metrics cm
    GROUP BY cm.date, cm.org_id, cm.device_id
)

SELECT
    date,
    org_id,
    device_id,
    obd_value_to_comparison_map,
    total_obd_values
FROM device_daily_aggregation
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily device-level aggregation of source comparison metrics for each OBD signal value. "
        "Reads per-source statistics from agg_signal_cache_sources_by_obd_value and compares them to calculate "
        "unified metrics that work for both monotonic signals (odometer) and variable signals (temperature): "
        "coefficient of variation (CV), range ratio, interquartile range (IQR), source agreement score, and "
        "max divergence percentage. Metrics are only calculated when multiple sources exist (source_count > 1). "
        "For single-source signals, divergence metrics are set to NULL. This enables identifying signals where "
        "sources disagree significantly, understanding source reliability, and analyzing population behavior patterns. "
        "This asset avoids duplicate reads from the expensive Kinesis source by leveraging the intermediate aggregation table.",
        row_meaning="Each row represents one device per day per OBD value with metrics comparing how different sources "
        "agree or differ in their reported values. Includes source_count, CV (normalized spread), range_ratio "
        "(normalized value spread), IQR (distribution width), agreement_score (percentage of sources within ±5% of median), "
        "max_divergence_pct (maximum difference as % of mean), and aggregate value statistics (mean, median, min, max).",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_SIGNAL_CACHE_SOURCES_BY_OBD_VALUE),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        min_workers=4,
        max_workers=16,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_SIGNAL_SOURCE_COMPARISON_DAILY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_signal_source_comparison_daily(context: AssetExecutionContext) -> str:
    """
    Generate daily device-level source comparison metrics for OBD signals.

    This asset reads from agg_signal_cache_sources_by_obd_value which contains per-source
    statistics (mean, median, min, max, p25, p75) for each unique source (bus_id, ecu_id,
    request_id, data_id combination). It extracts these per-source statistics and compares
    them to calculate unified metrics that work for both monotonic signals (odometer) and
    variable signals (temperature): coefficient of variation (CV), range ratio, interquartile
    range (IQR), source agreement score, and max divergence percentage. This avoids duplicate
    reads from the expensive Kinesis source by leveraging the intermediate aggregation table.

    Args:
        context: Dagster asset execution context providing partition and run information

    Returns:
        str: Formatted SQL query string for the current partition date range
    """
    return format_date_partition_query(QUERY, context)

