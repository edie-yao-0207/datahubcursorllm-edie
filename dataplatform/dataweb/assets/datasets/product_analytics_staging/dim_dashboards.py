"""
Dashboard dimension - canonical registry of dashboards by dashboard_id

Provides a single source of truth for dashboard metadata (workspace_id, raw_dashboard_id).
Derived from distinct dashboards in fct_databricks_dashboard_events.

This is the dimension table that owns:
- DASHBOARD_ID_COLUMN, DASHBOARD_ID_SQL - hash key for joining
- WORKSPACE_ID_COLUMN, RAW_DASHBOARD_ID_COLUMN - dashboard attributes

Downstream fact/aggregate tables should import these columns from here.

Grain: one row per unique dashboard (workspace_id + raw_dashboard_id) per date
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
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, Auditlog
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.firmware.schema import (
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    Column,
    ColumnType,
    DataType,
    Metadata,
)
from dataweb.userpkgs.firmware.hash_utils import hash_id_sql


# ============================================================================
# Dashboard ID Hash Key - owned by this dimension table
# ============================================================================

# Columns used to generate dashboard_id hash
DASHBOARD_ID_HASH_COLUMNS = ["workspace_id", "dashboard_id"]
DASHBOARD_ID_SQL = hash_id_sql(DASHBOARD_ID_HASH_COLUMNS)


# ============================================================================
# Reusable Column Definitions - import these in downstream tables
# ============================================================================

DASHBOARD_ID_COLUMN = Column(
    name="dashboard_id",
    type=DataType.LONG,
    nullable=False,
    primary_key=True,
    metadata=Metadata(
        comment="xxhash64(workspace_id|raw_dashboard_id) - FK to dim_dashboards"
    ),
)

WORKSPACE_ID_COLUMN = Column(
    name="workspace_id",
    type=DataType.STRING,
    nullable=False,
    metadata=Metadata(comment="Databricks workspace ID"),
)

RAW_DASHBOARD_ID_COLUMN = Column(
    name="raw_dashboard_id",
    type=DataType.STRING,
    nullable=False,
    metadata=Metadata(comment="Original dashboard ID string from Databricks"),
)


# ============================================================================
# Schema definition
# ============================================================================

COLUMNS = [
    ColumnType.DATE,
    DASHBOARD_ID_COLUMN,
    WORKSPACE_ID_COLUMN,
    RAW_DASHBOARD_ID_COLUMN,
    Column(
        name="first_seen_date",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="First date this dashboard had any events"),
    ),
    Column(
        name="daily_events",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Event count for this dashboard on this date"),
    ),
    Column(
        name="daily_unique_users",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Unique users who accessed this dashboard on this date"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Extract distinct dashboards with daily metrics
-- Date-partitioned like other assets for history
WITH daily_events AS (
  SELECT
    CAST(date AS DATE) AS date,
    workspace_id,
    dashboard_id,
    user_email
  FROM auditlog.fct_databricks_dashboard_events
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
    AND dashboard_id IS NOT NULL
    AND workspace_id IS NOT NULL
),

-- First seen date requires looking at all history
first_seen AS (
  SELECT
    workspace_id,
    dashboard_id,
    MIN(CAST(date AS DATE)) AS first_seen_date
  FROM auditlog.fct_databricks_dashboard_events
  WHERE dashboard_id IS NOT NULL
    AND workspace_id IS NOT NULL
  GROUP BY workspace_id, dashboard_id
),

-- Daily aggregates
daily_stats AS (
  SELECT
    date,
    workspace_id,
    dashboard_id,
    COUNT(*) AS daily_events,
    COUNT(DISTINCT user_email) AS daily_unique_users
  FROM daily_events
  GROUP BY date, workspace_id, dashboard_id
)

SELECT
  CAST(d.date AS STRING) AS date,
  {DASHBOARD_ID_SQL} AS dashboard_id,
  workspace_id,
  dashboard_id AS raw_dashboard_id,
  CAST(f.first_seen_date AS STRING) AS first_seen_date,
  d.daily_events,
  d.daily_unique_users
FROM daily_stats d
LEFT JOIN first_seen f
  USING (workspace_id, dashboard_id)
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Dashboard dimension - canonical registry of dashboards with dashboard_id for joining fact tables.",
        row_meaning="Each row represents a unique dashboard (workspace_id + raw_dashboard_id) with daily activity metrics.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Auditlog.FCT_DATABRICKS_DASHBOARD_EVENTS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_DASHBOARDS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_dashboards(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context, DASHBOARD_ID_SQL=DASHBOARD_ID_SQL)
