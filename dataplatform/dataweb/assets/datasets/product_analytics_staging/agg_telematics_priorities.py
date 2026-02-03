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
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.query import (
    format_date_partition_query,
)
from dataweb.userpkgs.firmware.table import (
    Definitions,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
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

QUERY = """
WITH data AS (
  SELECT
    coverage.date,
    coverage.type,
    issue.issue,
    coverage.grouping_hash,
    population.grouping_level,
    issue.count_distinct_device_id_with_issue AS missing_device_count,
    (1 - coverage.percent_coverage) AS coverage_gap_fraction,
    LOG(1 + missing_device_count) * coverage_gap_fraction AS scaled_gap_score,
    CASE
      WHEN priority.type IS NOT NULL THEN 0
      WHEN stat.default_priority = 0 THEN 1
      WHEN stat.default_priority > 0 THEN stat.default_priority
      ELSE 99
    END AS data_priority

  FROM {product_analytics_staging}.agg_telematics_actual_coverage_normalized AS coverage
  JOIN {product_analytics_staging}.agg_telematics_populations AS population USING (date, grouping_hash)
  JOIN {product_analytics_staging}.agg_telematics_issue_ranking AS issue USING (date, grouping_hash)
  LEFT JOIN definitions.telematics_market_priority AS priority USING (type, market, engine_type)
  LEFT JOIN {product_analytics_staging}.fct_telematics_stat_metadata AS stat USING (type)

  WHERE date BETWEEN "{date_start}" AND "{date_end}"
    AND issue.issue = "p0_diagnostic_gap"
    AND percent_coverage < 0.80
)

SELECT
  date,
  type,
  issue,
  grouping_hash,
  CAST(data_priority AS INTEGER) AS data_priority,
  missing_device_count,
  coverage_gap_fraction,
  scaled_gap_score,

  RANK() OVER (
    PARTITION BY grouping_level, type
    ORDER BY data_priority ASC, scaled_gap_score DESC 
  ) AS type_rank,

  RANK() OVER (
    PARTITION BY grouping_level
    ORDER BY data_priority ASC, scaled_gap_score DESC 
  ) AS overall_rank

FROM data
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.TYPE,
    ColumnType.ISSUE,
    ColumnType.GROUPING_HASH,
    Column(
        name="data_priority",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="The data priority of the device."),
    ),
    Column(
        name="missing_device_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The number of devices missing coverage."),
    ),
    Column(
        name="coverage_gap_fraction",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="The coverage gap fraction of the device."),
    ),
    Column(
        name="scaled_gap_score",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="The scaled gap score of the device."),
    ),
    Column(
        name="type_rank",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="The rank of the device by type."),
    ),
    Column(
        name="overall_rank",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="The rank of the device by area."),
    )
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics priorities.",
        row_meaning="The priorities of telematics devices by grouping level, product area, and sub-product area.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Definitions.TELEMATICS_MARKET_PRIORITY),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_STAT_METADATA),
        AnyUpstream(Definitions.TELEMATICS_ISSUE_RANK),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_POPULATIONS),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_ACTUAL_COVERAGE_NORMALIZED),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_ISSUE_RANKING),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_PRIORITIES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_priorities(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
