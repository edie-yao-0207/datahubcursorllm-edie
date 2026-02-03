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
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    ProductAnalytics,
    Definitions,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.userpkgs.query import format_agg_date_partition_query 
from .agg_telematics_populations import DIMENSIONS, GROUPINGS

QUERY = """
WITH types AS (
  SELECT DISTINCT type
  FROM product_analytics.agg_device_stats_secondary_coverage
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
),

population AS (
  SELECT *
  FROM product_analytics_staging.agg_telematics_populations
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
),

full_grid AS (
  SELECT
    p.date,
    p.grouping_hash,
    t.type,
    p.count_distinct_device_id AS population_count_distinct_device_id,
    p.count_ice,
    p.count_hydrogen,
    p.count_hybrid,
    p.count_bev,
    p.count_phev,
    p.count_unknown
  FROM population p
  CROSS JOIN types t
),

joined_coverage AS (
  SELECT
    date,
    grouping_hash,
    type,
    IF(metadata.is_applicable_ice, g.count_ice, 0)
    + IF(metadata.is_applicable_hydrogen, g.count_hydrogen, 0)
    + IF(metadata.is_applicable_hybrid, g.count_hybrid, 0)
    + IF(metadata.is_applicable_bev, g.count_bev, 0)
    + IF(metadata.is_applicable_phev, g.count_phev, 0)
    + IF(metadata.is_applicable_unknown, g.count_unknown, 0) AS population_count_distinct_device_id,

    IF(metadata.is_applicable_ice, c.count_ice, 0)
    + IF(metadata.is_applicable_hydrogen, c.count_hydrogen, 0)
    + IF(metadata.is_applicable_hybrid, c.count_hybrid, 0)
    + IF(metadata.is_applicable_bev, c.count_bev, 0)
    + IF(metadata.is_applicable_phev, c.count_phev, 0)
    + IF(metadata.is_applicable_unknown, c.count_unknown, 0) AS coverage_count_distinct_device_id,
    
    c.count_distinct_org_id AS coverage_count_distinct_org_id
  FROM full_grid g
  LEFT JOIN product_analytics_staging.agg_telematics_actual_coverage c
    USING (date, grouping_hash, type)
  JOIN product_analytics_staging.fct_telematics_stat_metadata AS metadata
    USING (type)
)

SELECT
  date,
  grouping_hash,
  type,
  population_count_distinct_device_id,
  coverage_count_distinct_device_id,
  coverage_count_distinct_org_id,
  COALESCE(coverage_count_distinct_device_id / population_count_distinct_device_id, 0.0) AS percent_coverage,
  CASE
    -- Low: (<50 total vehicles) OR (1 org AND <200 vehicles)
    WHEN COALESCE(coverage_count_distinct_device_id, 0) < 50 THEN 'Low'
    WHEN COALESCE(coverage_count_distinct_org_id, 0) = 1 AND COALESCE(coverage_count_distinct_device_id, 0) < 200 THEN 'Low'
    -- High: (200+ vehicles AND multiple orgs)
    WHEN COALESCE(coverage_count_distinct_device_id, 0) >= 200 AND COALESCE(coverage_count_distinct_org_id, 0) > 1 THEN 'High'
    -- Medium: (50–200 vehicles AND multiple orgs) OR (200+ vehicles AND 1 org)
    WHEN COALESCE(coverage_count_distinct_device_id, 0) BETWEEN 50 AND 199 AND COALESCE(coverage_count_distinct_org_id, 0) > 1 THEN 'Medium'
    WHEN COALESCE(coverage_count_distinct_device_id, 0) >= 200 AND COALESCE(coverage_count_distinct_org_id, 0) = 1 THEN 'Medium'
    -- Default to Low if no conditions match (e.g., null values)
    ELSE 'Low'
  END AS confidence
FROM joined_coverage
WHERE population_count_distinct_device_id > 0
"""

COLUMNS =  [
    ColumnType.DATE,
    ColumnType.GROUPING_HASH,
    ColumnType.TYPE,
    Column(
        name="population_count_distinct_device_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The number of active devices in the population."),
    ),
    Column(
        name="coverage_count_distinct_device_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The number of active devices with coverage in the population."),
    ),
    Column(
        name="coverage_count_distinct_org_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The number of distinct organizations with coverage in the population."),
    ),
    Column(
        name="percent_coverage",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="The coverage of the population."),
    ),
    Column(
        name="confidence",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Confidence level based on sample size and org diversity. Low: <50 vehicles OR (1 org AND <200 vehicles). Medium: (50-200 vehicles AND multiple orgs) OR (200+ vehicles AND 1 org). High: 200+ vehicles AND multiple orgs."),
    )
]
SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics coverage.",
        row_meaning="The coverage of populations of devices by type.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_ACTUAL_COVERAGE),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_POPULATIONS),
        AnyUpstream(ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_COVERAGE),
        AnyUpstream(Definitions.TELEMATICS_APPLICABLE_SIGNALS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_ACTUAL_COVERAGE_NORMALIZED.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_actual_coverage_normalized(context: AssetExecutionContext) -> str:
    return format_agg_date_partition_query(context, QUERY, DIMENSIONS, GROUPINGS)