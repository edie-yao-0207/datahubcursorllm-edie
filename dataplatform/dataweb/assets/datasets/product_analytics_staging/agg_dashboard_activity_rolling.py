"""
Rolling dashboard activity metrics per hierarchy level

Tracks dashboard popularity and usage patterns at multiple hierarchy levels.
Join to dim_dashboard_hierarchies on (date, dashboard_hierarchy_id) for hierarchy details.
Join to dim_dashboards on dashboard_id for dashboard details.

Hierarchy levels (from dim_dashboard_hierarchies.grouping_columns):
- 'overall' - All dashboards combined
- 'workspace_id' - Per workspace
- 'workspace_id.dashboard_id' - Per individual dashboard

Grain: one row per date + dashboard_hierarchy_id
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
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.firmware.schema import (
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    struct_with_comments,
    Column,
    ColumnType,
    DataType,
    Metadata,
)
from dataweb.userpkgs.firmware.hash_utils import (
    build_grouping_id_sql,
)

# Import from dimension tables
from dataweb.assets.datasets.product_analytics_staging.dim_dashboards import (
    DASHBOARD_ID_SQL,
)
from dataweb.assets.datasets.product_analytics_staging.dim_dashboard_hierarchies import (
    DASHBOARD_HIERARCHY_ID_COLUMN,
)


# ============================================================================
# Hierarchy Configuration (dashboard-specific)
# ============================================================================

# Dimension columns for GROUPING SETS (must match dim_dashboard_hierarchies)
HIERARCHY_COLUMNS = ["workspace_id", "dashboard_id"]
HIERARCHY_ID_SQL = build_grouping_id_sql(HIERARCHY_COLUMNS)


# ============================================================================
# Activity Metrics Struct
# ============================================================================

ACTIVITY_METRICS_STRUCT = struct_with_comments(
    ("unique_users", DataType.LONG, "Distinct users who accessed dashboards"),
    ("unique_dashboards", DataType.LONG, "Distinct dashboards accessed"),
    ("total_events", DataType.LONG, "Total dashboard events"),
    ("view_count", DataType.LONG, "Number of view actions"),
    ("clone_count", DataType.LONG, "Number of clone actions"),
    ("edit_count", DataType.LONG, "Number of edit/update actions"),
    ("active_days", DataType.LONG, "Days with at least one event"),
)


# ============================================================================
# Schema Definition - only dashboard_hierarchy_id (FK), no dimension columns
# ============================================================================

COLUMNS = [
    ColumnType.DATE,
    DASHBOARD_HIERARCHY_ID_COLUMN,
    Column(
        name="daily",
        type=ACTIVITY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Daily activity metrics"),
    ),
    Column(
        name="weekly",
        type=ACTIVITY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="7-day rolling activity metrics"),
    ),
    Column(
        name="monthly",
        type=ACTIVITY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="30-day rolling activity metrics"),
    ),
    Column(
        name="user_growth_pct",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Month-over-month user growth percentage"),
    ),
    Column(
        name="event_growth_pct",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Month-over-month event growth percentage"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Generate date spine for output dates
WITH date_spine AS (
  SELECT DISTINCT CAST(date AS DATE) AS date
  FROM {product_analytics_staging}.fct_dashboard_usage_daily
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
),

-- Get enriched events, joining to dim_dashboards to get workspace_id
enriched_events AS (
  SELECT
    CAST(f.date AS DATE) AS date,
    f.employee_email,
    d.workspace_id,
    f.dashboard_id,
    f.total_events,
    f.view_count,
    f.clone_count,
    f.edit_count
  FROM {product_analytics_staging}.fct_dashboard_usage_daily f
  INNER JOIN {product_analytics_staging}.dim_dashboards d
    USING (date, dashboard_id)
  WHERE f.date BETWEEN DATE_SUB(DATE("{date_start}"), 29) AND DATE("{date_end}")
),

-- Pre-join date spine with events for rolling window
date_window_data AS (
  SELECT
    d.date AS output_date,
    e.date AS event_date,
    e.employee_email,
    e.workspace_id,
    e.dashboard_id,
    e.total_events,
    e.view_count,
    e.clone_count,
    e.edit_count
  FROM date_spine d
  INNER JOIN enriched_events e
    ON e.date BETWEEN DATE_SUB(d.date, 29) AND d.date
),

-- Compute rolling metrics with GROUPING SETS
rolling_metrics AS (
  SELECT
    output_date AS date,
    
    -- Only dashboard_hierarchy_id - join to dim_dashboard_hierarchies for details
    {HIERARCHY_ID_SQL} AS dashboard_hierarchy_id,
    
    -- Daily metrics (same day only)
    COUNT(DISTINCT CASE WHEN event_date = output_date THEN employee_email END) AS daily_unique_users,
    COUNT(DISTINCT CASE WHEN event_date = output_date THEN dashboard_id END) AS daily_unique_dashboards,
    SUM(CASE WHEN event_date = output_date THEN total_events ELSE 0 END) AS daily_total_events,
    SUM(CASE WHEN event_date = output_date THEN view_count ELSE 0 END) AS daily_view_count,
    SUM(CASE WHEN event_date = output_date THEN clone_count ELSE 0 END) AS daily_clone_count,
    SUM(CASE WHEN event_date = output_date THEN edit_count ELSE 0 END) AS daily_edit_count,
    COUNT(DISTINCT CASE WHEN event_date = output_date THEN event_date END) AS daily_active_days,
    
    -- 7-day rolling metrics
    COUNT(DISTINCT CASE WHEN event_date BETWEEN DATE_SUB(output_date, 6) AND output_date THEN employee_email END) AS weekly_unique_users,
    COUNT(DISTINCT CASE WHEN event_date BETWEEN DATE_SUB(output_date, 6) AND output_date THEN dashboard_id END) AS weekly_unique_dashboards,
    SUM(CASE WHEN event_date BETWEEN DATE_SUB(output_date, 6) AND output_date THEN total_events ELSE 0 END) AS weekly_total_events,
    SUM(CASE WHEN event_date BETWEEN DATE_SUB(output_date, 6) AND output_date THEN view_count ELSE 0 END) AS weekly_view_count,
    SUM(CASE WHEN event_date BETWEEN DATE_SUB(output_date, 6) AND output_date THEN clone_count ELSE 0 END) AS weekly_clone_count,
    SUM(CASE WHEN event_date BETWEEN DATE_SUB(output_date, 6) AND output_date THEN edit_count ELSE 0 END) AS weekly_edit_count,
    COUNT(DISTINCT CASE WHEN event_date BETWEEN DATE_SUB(output_date, 6) AND output_date THEN event_date END) AS weekly_active_days,
    
    -- 30-day rolling metrics
    COUNT(DISTINCT employee_email) AS monthly_unique_users,
    COUNT(DISTINCT dashboard_id) AS monthly_unique_dashboards,
    SUM(total_events) AS monthly_total_events,
    SUM(view_count) AS monthly_view_count,
    SUM(clone_count) AS monthly_clone_count,
    SUM(edit_count) AS monthly_edit_count,
    COUNT(DISTINCT event_date) AS monthly_active_days
    
  FROM date_window_data
  GROUP BY GROUPING SETS (
    (output_date),                              -- Overall
    (output_date, workspace_id),                -- Per workspace
    (output_date, workspace_id, dashboard_id)   -- Per dashboard
  )
),

-- Add growth metrics
with_growth AS (
  SELECT
    *,
    -- Month-over-month growth
    (monthly_unique_users - LAG(monthly_unique_users, 30) OVER (
      PARTITION BY dashboard_hierarchy_id ORDER BY date
    )) * 100.0 / NULLIF(LAG(monthly_unique_users, 30) OVER (
      PARTITION BY dashboard_hierarchy_id ORDER BY date
    ), 0) AS user_growth_pct,
    
    (monthly_total_events - LAG(monthly_total_events, 30) OVER (
      PARTITION BY dashboard_hierarchy_id ORDER BY date
    )) * 100.0 / NULLIF(LAG(monthly_total_events, 30) OVER (
      PARTITION BY dashboard_hierarchy_id ORDER BY date
    ), 0) AS event_growth_pct
  FROM rolling_metrics
)

SELECT
  CAST(date AS STRING) AS date,
  dashboard_hierarchy_id,
  
  NAMED_STRUCT(
    'unique_users', CAST(daily_unique_users AS LONG),
    'unique_dashboards', CAST(daily_unique_dashboards AS LONG),
    'total_events', CAST(daily_total_events AS LONG),
    'view_count', CAST(daily_view_count AS LONG),
    'clone_count', CAST(daily_clone_count AS LONG),
    'edit_count', CAST(daily_edit_count AS LONG),
    'active_days', CAST(daily_active_days AS LONG)
  ) AS daily,
  
  NAMED_STRUCT(
    'unique_users', CAST(weekly_unique_users AS LONG),
    'unique_dashboards', CAST(weekly_unique_dashboards AS LONG),
    'total_events', CAST(weekly_total_events AS LONG),
    'view_count', CAST(weekly_view_count AS LONG),
    'clone_count', CAST(weekly_clone_count AS LONG),
    'edit_count', CAST(weekly_edit_count AS LONG),
    'active_days', CAST(weekly_active_days AS LONG)
  ) AS weekly,
  
  NAMED_STRUCT(
    'unique_users', CAST(monthly_unique_users AS LONG),
    'unique_dashboards', CAST(monthly_unique_dashboards AS LONG),
    'total_events', CAST(monthly_total_events AS LONG),
    'view_count', CAST(monthly_view_count AS LONG),
    'clone_count', CAST(monthly_clone_count AS LONG),
    'edit_count', CAST(monthly_edit_count AS LONG),
    'active_days', CAST(monthly_active_days AS LONG)
  ) AS monthly,
  
  CAST(user_growth_pct AS DOUBLE) AS user_growth_pct,
  CAST(event_growth_pct AS DOUBLE) AS event_growth_pct

FROM with_growth
WHERE date BETWEEN "{date_start}" AND "{date_end}"
ORDER BY date, dashboard_hierarchy_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Rolling dashboard activity metrics. Join to dim_dashboard_hierarchies for hierarchy details.",
        row_meaning="Each row represents activity metrics for a specific dashboard_hierarchy_id (overall/workspace/dashboard) on a date.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_DASHBOARD_USAGE_DAILY),
        AnyUpstream(ProductAnalyticsStaging.DIM_DASHBOARDS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_DASHBOARD_ACTIVITY_ROLLING.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_dashboard_activity_rolling(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY, context, 
        DASHBOARD_ID_SQL=DASHBOARD_ID_SQL,
        HIERARCHY_ID_SQL=HIERARCHY_ID_SQL,
    )
