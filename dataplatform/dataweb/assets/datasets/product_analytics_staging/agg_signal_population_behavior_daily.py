"""
Daily Signal Population Behavior Metrics

This asset creates a daily population-level aggregation showing how signal sources behave across devices
within the same MMYEF + cable + product (platform) group. Since cable type and platform influence which
sources are available to a device, we aggregate by MMYEF + cable + product to ensure we're comparing
devices with similar source availability.

Each row represents one MMYEF + cable + product combination per day with a map from OBD value to struct
containing population-level metrics: average source comparison metrics across devices, multi-source prevalence,
source diversity index, and population consistency (how much values vary across devices in the population).
This structure prevents row explosion (one row per MMYEF + cable + product per day instead of one row per
MMYEF + cable + product + OBD value). Use MAP_KEYS(obd_value_to_behavior_map) to get all OBD values, or
obd_value_to_behavior_map[obd_value] to get the metrics for a specific signal.

This enables understanding how populations behave, identifying signals with high source disagreement,
and comparing behavior patterns across different vehicle types, cables, and platforms.
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
from dataweb.userpkgs.firmware.table import ProductAnalytics, ProductAnalyticsStaging
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
    Column(
        name="mmyef_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="XXHASH64 of vehicle characteristics (Make, Model, Year, Engine, Fuel). See product_analytics_staging.dim_device_vehicle_properties for hash expression.",
        ),
    ),
    ColumnType.CABLE_ID,
    ColumnType.PRODUCT_ID,
    Column(
        name="obd_value_to_behavior_map",
        type=map_of(
            DataType.LONG,  # obd_value as key
            struct_with_comments(
                ("device_count", DataType.LONG, "Number of distinct devices in this MMYEF + cable + product group with this OBD signal"),
                ("avg_cv", DataType.DOUBLE, "Average coefficient of variation across devices (only devices with multiple sources). NULL if no devices have multiple sources"),
                ("avg_source_count", DataType.DOUBLE, "Average number of sources per device in this population"),
                ("multi_source_pct", DataType.DOUBLE, "Percentage of devices with multiple sources (source_count > 1)"),
                ("population_cv", DataType.DOUBLE, "Coefficient of variation of device median values across the population. Measures population consistency. NULL if mean = 0 or only one device"),
                ("source_diversity_index", DataType.DOUBLE, "Average number of unique sources per device in this population. Higher values indicate more source diversity"),
                ("avg_mean_value", DataType.DOUBLE, "Average mean value across all devices in the population"),
                ("avg_median_value", DataType.DOUBLE, "Average median value across all devices in the population"),
                ("min_device_median", DataType.DOUBLE, "Minimum device median value in the population"),
                ("max_device_median", DataType.DOUBLE, "Maximum device median value in the population"),
            ),
            value_contains_null=True,
        ),
        nullable=False,
        metadata=Metadata(
            comment="Map from OBD signal value (obd_value) to struct containing population behavior metrics: device_count, avg_cv, avg_source_count, multi_source_pct, population_cv, source_diversity_index, and aggregate value statistics (avg_mean, avg_median, min/max device medians). Use MAP_KEYS(obd_value_to_behavior_map) to get all OBD values, or obd_value_to_behavior_map[obd_value] to get the metrics for a specific signal. This structure prevents row explosion (one row per MMYEF + cable + product per day instead of one row per MMYEF + cable + product + OBD value).",
        ),
    ),
    Column(
        name="total_obd_values",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total number of unique OBD signal values (keys in obd_value_to_behavior_map) for this MMYEF + cable + product group on this day.",
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """
WITH latest_vehicle_properties AS (
    SELECT
        org_id,
        device_id,
        mmyef_id
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
        AND mmyef_id IS NOT NULL
),

latest_device_dimensions AS (
    SELECT
        org_id,
        device_id,
        cable_id,
        product_id
    FROM product_analytics.dim_device_dimensions
    WHERE date = (SELECT MAX(date) FROM product_analytics.dim_device_dimensions)
        AND cable_id IS NOT NULL
        AND product_id IS NOT NULL
),

-- Explode the map to get individual OBD values with their comparison metrics
device_comparison_exploded AS (
    SELECT
        sc.date,
        sc.org_id,
        sc.device_id,
        obd_value_key AS obd_value,
        comparison_data.source_count,
        comparison_data.cv,
        comparison_data.range_ratio,
        comparison_data.iqr,
        comparison_data.agreement_score,
        comparison_data.max_divergence_pct,
        comparison_data.mean_value,
        comparison_data.median_value,
        comparison_data.min_value,
        comparison_data.max_value
    FROM {product_analytics_staging}.agg_signal_source_comparison_daily sc
    LATERAL VIEW explode(obd_value_to_comparison_map) AS obd_value_key, comparison_data
    WHERE sc.date BETWEEN "{date_start}" AND "{date_end}"
        AND comparison_data IS NOT NULL
),

device_with_dimensions AS (
    SELECT
        dce.date,
        dce.org_id,
        dce.device_id,
        dce.obd_value,
        dce.source_count,
        dce.cv,
        dce.range_ratio,
        dce.iqr,
        dce.agreement_score,
        dce.max_divergence_pct,
        dce.mean_value,
        dce.median_value,
        dce.min_value,
        dce.max_value,
        vp.mmyef_id,
        dd.cable_id,
        dd.product_id
    FROM device_comparison_exploded dce
    LEFT JOIN latest_vehicle_properties vp
        ON dce.org_id = vp.org_id
        AND dce.device_id = vp.device_id
    LEFT JOIN latest_device_dimensions dd
        ON dce.org_id = dd.org_id
        AND dce.device_id = dd.device_id
    WHERE vp.mmyef_id IS NOT NULL
        AND dd.cable_id IS NOT NULL
        AND dd.product_id IS NOT NULL
),

population_aggregation AS (
    SELECT
        date,
        mmyef_id,
        cable_id,
        product_id,
        obd_value,
        COUNT(DISTINCT device_id) AS device_count,
        -- Average CV across devices (only devices with multiple sources)
        AVG(CASE WHEN source_count > 1 THEN cv ELSE NULL END) AS avg_cv,
        -- Average source count per device
        AVG(source_count) AS avg_source_count,
        -- Percentage of devices with multiple sources
        CAST(SUM(CASE WHEN source_count > 1 THEN 1 ELSE 0 END) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE) * 100.0 AS multi_source_pct,
        -- Population CV: CV of device median values
        CASE
            WHEN COUNT(*) > 1 AND AVG(median_value) != 0 AND AVG(median_value) IS NOT NULL THEN
                STDDEV_POP(median_value) / ABS(AVG(median_value))
            ELSE NULL
        END AS population_cv,
        -- Source diversity index: average unique sources per device
        AVG(source_count) AS source_diversity_index,
        -- Aggregate value statistics
        AVG(mean_value) AS avg_mean_value,
        AVG(median_value) AS avg_median_value,
        MIN(median_value) AS min_device_median,
        MAX(median_value) AS max_device_median
    FROM device_with_dimensions
    GROUP BY date, mmyef_id, cable_id, product_id, obd_value
),

mmyef_daily_aggregation AS (
    SELECT
        pa.date,
        pa.mmyef_id,
        pa.cable_id,
        pa.product_id,
        MAP_FROM_ENTRIES(
            COLLECT_LIST(
                STRUCT(
                    pa.obd_value AS key,
                    STRUCT(
                        pa.device_count,
                        pa.avg_cv,
                        pa.avg_source_count,
                        pa.multi_source_pct,
                        pa.population_cv,
                        pa.source_diversity_index,
                        pa.avg_mean_value,
                        pa.avg_median_value,
                        pa.min_device_median,
                        pa.max_device_median
                    ) AS value
                )
            )
        ) AS obd_value_to_behavior_map,
        CAST(COUNT(DISTINCT pa.obd_value) AS BIGINT) AS total_obd_values
    FROM population_aggregation pa
    GROUP BY pa.date, pa.mmyef_id, pa.cable_id, pa.product_id
)

SELECT
    date,
    mmyef_id,
    cable_id,
    product_id,
    obd_value_to_behavior_map,
    total_obd_values
FROM mmyef_daily_aggregation
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily population-level aggregation of signal source behavior metrics grouped by MMYEF + cable + product. "
        "Since cable type and platform influence which sources are available to a device, we aggregate by MMYEF + cable + product "
        "to ensure we're comparing devices with similar source availability. Shows how signal sources behave across devices "
        "within the same vehicle type, cable, and platform combination: average source comparison metrics, multi-source prevalence, "
        "source diversity index, and population consistency (how much values vary across devices). "
        "Stored as a map from obd_value to struct containing all population behavior metrics. This structure prevents row explosion "
        "(one row per MMYEF + cable + product per day instead of one row per MMYEF + cable + product + OBD value) and enables efficient analysis. "
        "Use MAP_KEYS(obd_value_to_behavior_map) to get all OBD values, or obd_value_to_behavior_map[obd_value] to get the metrics "
        "for a specific signal. This enables understanding how populations behave, identifying signals with high source disagreement, "
        "and comparing behavior patterns across different vehicle types, cables, and platforms.",
        row_meaning="Each row represents one MMYEF + cable + product combination per day with a map from OBD value to struct containing "
        "population-level metrics: device_count (number of devices), avg_cv (average coefficient of variation), avg_source_count "
        "(average sources per device), multi_source_pct (percentage of devices with multiple sources), population_cv (consistency across devices), "
        "source_diversity_index (average unique sources per device), and aggregate value statistics (avg_mean, avg_median, min/max device medians). "
        "This structure prevents row explosion and enables efficient queries across all signals with full population context.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_SIGNAL_SOURCE_COMPARISON_DAILY),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
        AnyUpstream(ProductAnalytics.DIM_DEVICE_DIMENSIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        min_workers=4,
        max_workers=16,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_SIGNAL_POPULATION_BEHAVIOR_DAILY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_signal_population_behavior_daily(context: AssetExecutionContext) -> str:
    """
    Generate daily population-level behavior metrics for signal sources.

    This asset aggregates device-level source comparison metrics at the MMYEF + cable + product level,
    showing how signal sources behave across devices within the same vehicle type, cable, and platform
    combination. This enables population-level analysis and comparison.

    Args:
        context: Dagster asset execution context providing partition and run information

    Returns:
        str: Formatted SQL query string for the current partition date range
    """
    return format_date_partition_query(QUERY, context)

