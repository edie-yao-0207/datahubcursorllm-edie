"""
Dashboard hierarchies dimension table (date-partitioned)

Defines aggregation populations for dashboard analysis by creating consistent hierarchy
identifiers. Uses workspace_id and dashboard_id as dimension columns.

Date-partitioned to support backfilling and historical lookups. Join on date + dashboard_hierarchy_id.

Two ways to select populations:

1. `grouping_columns` - Which dimension COLUMNS are in the grouping:
   - 'overall' = all dashboards aggregated
   - 'workspace_id' = per-workspace rollup
   - 'workspace_id.dashboard_id' = per-dashboard grain

2. `hierarchy_label` - The actual VALUES for those columns:
   - 'overall' = everything
   - '1234567890' = workspace-level with workspace_id='1234567890'
   - '1234567890.abc123' = specific dashboard

Example queries:
  -- All dashboard-level groupings
  WHERE grouping_columns = 'workspace_id.dashboard_id'

  -- Workspace-level rollup
  WHERE grouping_columns = 'workspace_id'

  -- Overall
  WHERE grouping_columns = 'overall'
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    AWSRegion,
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
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.firmware.schema import (
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    ColumnType,
    Metadata,
)
from dataclasses import replace
from dataweb.userpkgs.firmware.hash_utils import (
    build_grouping_id_sql,
    build_grouping_label_sql,
    build_grouping_columns_sql,
    HIERARCHY_ID_COLUMN,
    GROUPING_COLUMNS_COLUMN,
    HIERARCHY_LABEL_COLUMN,
)
from dataweb.assets.datasets.product_analytics_staging.dim_dashboards import (
    DASHBOARD_ID_COLUMN,
    WORKSPACE_ID_COLUMN,
)


# ============================================================================
# Hierarchy Configuration
# ============================================================================

# Dashboard-specific hierarchy ID - rename from generic HIERARCHY_ID_COLUMN
DASHBOARD_HIERARCHY_ID_COLUMN = replace(
    HIERARCHY_ID_COLUMN,
    name="dashboard_hierarchy_id",
    metadata=Metadata(comment="xxhash64 of dashboard dimension values - FK to dim_dashboard_hierarchies"),
)

# Dimension columns for GROUPING SETS (least to most granular)
HIERARCHY_COLUMNS = ["workspace_id", "dashboard_id"]

# Pre-built SQL for hierarchy columns
HIERARCHY_ID_SQL = build_grouping_id_sql(HIERARCHY_COLUMNS)
HIERARCHY_LABEL_SQL = build_grouping_label_sql(HIERARCHY_COLUMNS)
GROUPING_COLUMNS_SQL = build_grouping_columns_sql(HIERARCHY_COLUMNS)


# ============================================================================
# Schema Definition
# ============================================================================

# Nullable versions for hierarchy table (NULL for higher aggregation levels)
WORKSPACE_ID_NULLABLE = replace(WORKSPACE_ID_COLUMN, nullable=True, primary_key=False)
DASHBOARD_ID_NULLABLE = replace(DASHBOARD_ID_COLUMN, nullable=True, primary_key=False)

# Import Column and DataType for population count columns
from dataweb.userpkgs.firmware.schema import Column, DataType

# Population count columns - help understand hierarchy cardinality
DASHBOARD_COUNT_COLUMN = Column(
    name="dashboard_count",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of dashboards in this hierarchy grouping"),
)

DISTINCT_WORKSPACES_COLUMN = Column(
    name="distinct_workspaces",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of distinct workspaces in this grouping"),
)


COLUMNS = [
    ColumnType.DATE,
    DASHBOARD_HIERARCHY_ID_COLUMN,
    GROUPING_COLUMNS_COLUMN,
    HIERARCHY_LABEL_COLUMN,
    WORKSPACE_ID_NULLABLE,
    DASHBOARD_ID_NULLABLE,
    # Population counts
    DASHBOARD_COUNT_COLUMN,
    DISTINCT_WORKSPACES_COLUMN,
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Get distinct dimension combinations from dashboards per date
-- Uses GROUPING SETS to create overall, workspace, and dashboard level hierarchies
-- Includes population counts for understanding hierarchy cardinality
WITH dashboard_dims AS (
  SELECT DISTINCT
    date,
    workspace_id,
    dashboard_id
  FROM {product_analytics_staging}.dim_dashboards
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
)

SELECT
  CAST(date AS STRING) AS date,
  {HIERARCHY_ID_SQL} AS dashboard_hierarchy_id,
  {GROUPING_COLUMNS_SQL} AS grouping_columns,
  {HIERARCHY_LABEL_SQL} AS hierarchy_label,
  workspace_id,
  dashboard_id,
  -- Population counts help understand hierarchy cardinality
  COUNT(DISTINCT dashboard_id) AS dashboard_count,
  COUNT(DISTINCT workspace_id) AS distinct_workspaces
FROM dashboard_dims
GROUP BY GROUPING SETS (
  (date),                              -- Overall
  (date, workspace_id),                -- Per workspace  
  (date, workspace_id, dashboard_id)   -- Per dashboard
)
ORDER BY date, grouping_columns, hierarchy_label
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Dashboard hierarchies dimension - defines aggregation levels for dashboard analytics.",
        row_meaning="Each row represents a unique hierarchy level (overall/workspace/dashboard) for a date.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_DASHBOARDS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_DASHBOARD_HIERARCHIES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_dashboard_hierarchies(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY, context,
        HIERARCHY_ID_SQL=HIERARCHY_ID_SQL,
        HIERARCHY_LABEL_SQL=HIERARCHY_LABEL_SQL,
        GROUPING_COLUMNS_SQL=GROUPING_COLUMNS_SQL,
    )

