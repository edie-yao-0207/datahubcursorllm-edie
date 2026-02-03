"""
Telematics Data Quality Cutoffs Asset

This asset generates cutoffs for telematics values and deltas based on observed data.
It uses percentile and IQR-based methods to determine lower and upper cutoffs for both values and their deltas
to support data quality monitoring and anomaly detection.
"""

from dataclasses import replace
from pyspark.sql import DataFrame
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
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    Metadata,
    DataType,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from .fct_telematics_stat_metadata import TableColumn as StatMetadataColumns
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
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
WITH quantiles AS (
  SELECT
    date,
    grouping_hash,
    population_id,
    type,

    value_quantiles[0] AS val_p1,
    value_quantiles[24] AS val_p25,
    value_quantiles[74] AS val_p75,
    value_quantiles[98] AS val_p99,

    val_p75 - val_p25 AS val_iqr,
    val_p25 - 1.5 * val_iqr AS val_q_lower,
    val_p75 + 1.5 * val_iqr AS val_q_upper,

    delta_quantiles[0] AS delta_p1,
    delta_quantiles[24] AS delta_p25,
    delta_quantiles[74] AS delta_p75,
    delta_quantiles[98] AS delta_p99,

    delta_p75 - delta_p25 AS delta_iqr,
    delta_p25 - 1.5 * delta_iqr AS delta_q_lower,
    delta_p75 + 1.5 * delta_iqr AS delta_q_upper

  FROM {product_analytics_staging}.agg_population_quantile_rollup
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
    AND type = '{type}'
),

cutoffs AS (
  SELECT
    date,
    grouping_hash,
    population_id,
    type,
    LEAST(val_p1, val_q_lower) AS val_lower_cutoff,
    GREATEST(val_p99, val_q_upper) AS val_upper_cutoff,
    LEAST(delta_p1, delta_q_lower) AS delta_lower_cutoff,
    GREATEST(delta_p99, delta_q_upper) AS delta_upper_cutoff
  FROM quantiles
  WHERE type = '{type}'
),

operational_limits AS (
  SELECT
    type,
    value_min,
    value_max,
    delta_min,
    delta_max
  FROM {product_analytics_staging}.fct_telematics_stat_metadata
)

SELECT
  cutoffs.date,
  cutoffs.grouping_hash,
  cutoffs.population_id,
  cutoffs.type,
  cutoffs.val_lower_cutoff,
  cutoffs.val_upper_cutoff,
  cutoffs.delta_lower_cutoff,
  cutoffs.delta_upper_cutoff,
  CAST(operational_limits.value_min AS LONG) as value_min,
  operational_limits.value_max,
  CAST(operational_limits.delta_min AS LONG) as delta_min,
  CAST(operational_limits.delta_max AS LONG) as delta_max,
  GREATEST(val_lower_cutoff, value_min) as value_min_adjusted,
  LEAST(val_upper_cutoff, value_max) as value_max_adjusted,
  GREATEST(delta_lower_cutoff, delta_min) as delta_min_adjusted,
  LEAST(delta_upper_cutoff, delta_max) as delta_max_adjusted
FROM cutoffs
LEFT JOIN operational_limits USING (type)
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.GROUPING_HASH,
    ColumnType.POPULATION_ID,
    ColumnType.TYPE,
    Column(
        name="val_lower_cutoff",
        type=DataType.DOUBLE,
        metadata=Metadata(
            comment="Lower cutoff for metric values based on p1 percentile and IQR lower bound",
        ),
        nullable=True,
    ),
    Column(
        name="val_upper_cutoff",
        type=DataType.DOUBLE,
        metadata=Metadata(
            comment="Upper cutoff for metric values based on p99 percentile and IQR upper bound",
        ),
        nullable=True,
    ),
    Column(
        name="delta_lower_cutoff",
        type=DataType.DOUBLE,
        metadata=Metadata(
            comment="Lower cutoff for metric deltas based on p1 percentile and IQR lower bound",
        ),
        nullable=True,
    ),
    Column(
        name="delta_upper_cutoff",
        type=DataType.DOUBLE,
        metadata=Metadata(
            comment="Upper cutoff for metric deltas based on p99 percentile and IQR upper bound",
        ),
        nullable=True,
    ),
    # Import from fct_telematics_stat_metadata and override types as needed
    replace(StatMetadataColumns.VALUE_MIN.value, type=DataType.LONG),
    StatMetadataColumns.VALUE_MAX.value,
    replace(StatMetadataColumns.DELTA_MIN.value, type=DataType.LONG),
    replace(StatMetadataColumns.DELTA_MAX.value, type=DataType.LONG),
    Column(
        name="value_min_adjusted",
        type=DataType.DOUBLE,
        metadata=Metadata(
            comment="Lower adjusted cutoff for metric values based on operational limits and IQR lower bound",
        ),
        nullable=True,
    ),
    Column(
        name="value_max_adjusted",
        type=DataType.DOUBLE,
        metadata=Metadata(
            comment="Upper adjusted cutoff for metric values based on operational limits and IQR upper bound",
        ),
        nullable=True,
    ),
    Column(
        name="delta_min_adjusted",
        type=DataType.DOUBLE,
        metadata=Metadata(
            comment="Lower adjusted cutoff for metric deltas based on operational limits and IQR lower bound",
        ),
        nullable=True,
    ),
    Column(
        name="delta_max_adjusted",
        type=DataType.DOUBLE,
        metadata=Metadata(
            comment="Upper adjusted cutoff for metric deltas based on operational limits and IQR upper bound",
        ),
        nullable=True,
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Generate data quality cutoffs for telematics metrics using percentile and IQR methods for anomaly detection",
        row_meaning="Each row represents statistical cutoffs for a specific telematics metric and population on a given date",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=PARTITIONS,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_POPULATION_QUANTILE_ROLLUP),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_STAT_METADATA),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_DATA_QUALITY_CUTOFFS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_data_quality_cutoffs(context: AssetExecutionContext) -> DataFrame:
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
