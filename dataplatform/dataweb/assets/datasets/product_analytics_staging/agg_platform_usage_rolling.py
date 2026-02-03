"""
Rolling window platform usage metrics combining SQL queries and dashboard activity

Provides a unified view of data platform usage by combining:
- SQL query activity (derived from fct_table_user_queries)
- Dashboard engagement (from fct_dashboard_usage_daily)

Employee metadata from edw.silver.employee_hierarchy_active_vw.
Aggregated by user_hierarchy_id using hierarchy: job_family_group → job_family → department → employee.
Join to dim_user_hierarchies on (date, user_hierarchy_id) for hierarchy details.

Key metrics:
- total_active_users: Employees with ANY platform activity (SQL OR dashboard)
- sql_only_users: Employees who only wrote queries (no dashboard activity)
- dashboard_only_users: Employees who only viewed dashboards (no SQL activity)
- both_users: Employees engaged with both SQL queries AND dashboards

Grain: one row per date + user_hierarchy_id
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
    array_of,
)
from dataweb.assets.datasets.product_analytics_staging.dim_user_hierarchies import (
    USER_HIERARCHY_ID_COLUMN,
    HIERARCHY_ID_SQL,
)


# ============================================================================
# Metrics Structs - Split by Domain
# ============================================================================

# User segmentation counts
USER_COUNTS_STRUCT = struct_with_comments(
    ("total", DataType.LONG, "Employees with ANY platform activity (SQL OR dashboard)"),
    ("sql_users", DataType.LONG, "Employees who executed at least one SQL query"),
    ("dashboard_users", DataType.LONG, "Employees who had at least one dashboard interaction"),
    ("sql_only", DataType.LONG, "Employees with SQL activity but NO dashboard activity"),
    ("dashboard_only", DataType.LONG, "Employees with dashboard activity but NO SQL activity"),
    ("both", DataType.LONG, "Employees with BOTH SQL and dashboard activity"),
)

# SQL-specific metrics
SQL_METRICS_STRUCT = struct_with_comments(
    ("queries", DataType.LONG, "Total SQL queries executed"),
    ("unique_tables", DataType.LONG, "Distinct tables accessed via SQL"),
)

# Dashboard-specific metrics
DASHBOARD_METRICS_STRUCT = struct_with_comments(
    ("events", DataType.LONG, "Total dashboard interactions"),
    ("views", DataType.LONG, "Dashboard view actions"),
    ("clones", DataType.LONG, "Dashboard clone actions"),
    ("edits", DataType.LONG, "Dashboard edit/update actions"),
    ("unique_dashboards", DataType.LONG, "Distinct dashboards accessed"),
)

# Combined/overall metrics
COMBINED_METRICS_STRUCT = struct_with_comments(
    ("total_interactions", DataType.LONG, "Sum of SQL queries + dashboard events"),
    ("active_days", DataType.LONG, "Days with at least one interaction"),
    ("delta_users", DataType.LONG, "Change in active users from previous period"),
)

# Top-level struct grouping all domains
PLATFORM_USAGE_STRUCT = struct_with_comments(
    ("users", USER_COUNTS_STRUCT, "User segmentation metrics"),
    ("sql", SQL_METRICS_STRUCT, "SQL query metrics"),
    ("dashboard", DASHBOARD_METRICS_STRUCT, "Dashboard engagement metrics"),
    ("combined", COMBINED_METRICS_STRUCT, "Combined platform metrics"),
)


# ============================================================================
# Schema Definition
# ============================================================================

COLUMNS = [
    ColumnType.DATE,
    USER_HIERARCHY_ID_COLUMN,
    Column(
        name="daily",
        type=PLATFORM_USAGE_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Daily platform usage metrics"),
    ),
    Column(
        name="weekly",
        type=PLATFORM_USAGE_STRUCT,
        nullable=True,
        metadata=Metadata(comment="7-day rolling platform usage metrics"),
    ),
    Column(
        name="monthly",
        type=PLATFORM_USAGE_STRUCT,
        nullable=True,
        metadata=Metadata(comment="30-day rolling platform usage metrics"),
    ),
    Column(
        name="quarterly",
        type=PLATFORM_USAGE_STRUCT,
        nullable=True,
        metadata=Metadata(comment="90-day rolling platform usage metrics"),
    ),
    Column(
        name="yearly",
        type=PLATFORM_USAGE_STRUCT,
        nullable=True,
        metadata=Metadata(comment="365-day rolling platform usage metrics"),
    ),
    Column(
        name="top_dashboards_daily",
        type=array_of(DataType.LONG),
        nullable=True,
        metadata=Metadata(
            comment="Top 5 dashboard hash IDs accessed today. Join to dim_dashboards for names."
        ),
    ),
    Column(
        name="top_dashboards_monthly",
        type=array_of(DataType.LONG),
        nullable=True,
        metadata=Metadata(
            comment="Top 5 dashboard hash IDs in last 30 days. Join to dim_dashboards for names."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


# ============================================================================
# SQL Query
# ============================================================================

QUERY = r"""
-- Combine SQL and dashboard usage using fact tables
-- SQL: fct_table_user_queries (pure star schema - join to employee_hierarchy for attrs)
-- Dashboard: fct_dashboard_usage_daily (has denormalized employee attrs)

-- SQL query activity per employee per day (derived from fct_table_user_queries)
WITH sql_activity AS (
  SELECT
    q.date,
    q.employee_id,
    e.employee_email,
    e.job_family_group,
    e.job_family,
    e.department,
    SUM(q.query_count) AS query_count,
    COUNT(DISTINCT q.table_id) AS unique_tables
  FROM {product_analytics_staging}.fct_table_user_queries q
  JOIN edw.silver.employee_hierarchy_active_vw e ON q.employee_id = e.employee_id
  WHERE q.date BETWEEN DATE_SUB(DATE('{date_start}'), 364) AND DATE('{date_end}')
  GROUP BY q.date, q.employee_id, e.employee_email, e.job_family_group, e.job_family, e.department
),

-- Dashboard activity per employee per day (from fct_dashboard_usage_daily, aggregated by employee)
dashboard_activity AS (
  SELECT
    d.date,
    d.employee_id,
    MAX(d.employee_email) AS employee_email,
    MAX(d.job_family_group) AS job_family_group,
    MAX(d.job_family) AS job_family,
    MAX(d.department) AS department,
    SUM(d.total_events) AS event_count,
    SUM(d.view_count) AS view_count,
    SUM(d.clone_count) AS clone_count,
    SUM(d.edit_count) AS edit_count,
    COUNT(DISTINCT d.dashboard_id) AS unique_dashboards,
    COLLECT_LIST(STRUCT(d.dashboard_id, d.total_events AS event_count)) AS dashboard_counts
  FROM {product_analytics_staging}.fct_dashboard_usage_daily d
  WHERE d.date BETWEEN DATE_SUB(DATE('{date_start}'), 364) AND DATE('{date_end}')
  GROUP BY d.date, d.employee_id
),

-- Full outer join to combine both sources
combined_daily AS (
  SELECT
    COALESCE(s.date, d.date) AS date,
    COALESCE(s.employee_id, d.employee_id) AS employee_id,
    COALESCE(s.employee_email, d.employee_email) AS employee_email,
    COALESCE(s.job_family_group, d.job_family_group) AS job_family_group,
    COALESCE(s.job_family, d.job_family) AS job_family,
    COALESCE(s.department, d.department) AS department,
    COALESCE(s.query_count, 0) AS sql_queries,
    COALESCE(s.unique_tables, 0) AS unique_tables,
    COALESCE(d.event_count, 0) AS dashboard_events,
    COALESCE(d.view_count, 0) AS dashboard_view_count,
    COALESCE(d.clone_count, 0) AS dashboard_clone_count,
    COALESCE(d.edit_count, 0) AS dashboard_edit_count,
    COALESCE(d.unique_dashboards, 0) AS unique_dashboards,
    d.dashboard_counts,
    CASE WHEN s.employee_id IS NOT NULL THEN 1 ELSE 0 END AS has_sql,
    CASE WHEN d.employee_id IS NOT NULL THEN 1 ELSE 0 END AS has_dashboard
  FROM sql_activity s
  FULL OUTER JOIN dashboard_activity d
    USING (date, employee_id)
),

-- Aggregate at hierarchy levels using GROUPING SETS
-- job_family_group → job_family → department → employee_email
daily_agg AS (
  SELECT
    date,
    job_family_group,
    job_family,
    department,
    employee_email,
    {HIERARCHY_ID_SQL} AS user_hierarchy_id,
    
    -- User counts
    COUNT(DISTINCT employee_email) AS total_active_users,
    COUNT(DISTINCT CASE WHEN has_sql = 1 THEN employee_email END) AS sql_users,
    COUNT(DISTINCT CASE WHEN has_dashboard = 1 THEN employee_email END) AS dashboard_users,
    COUNT(DISTINCT CASE WHEN has_sql = 1 AND has_dashboard = 0 THEN employee_email END) AS sql_only_users,
    COUNT(DISTINCT CASE WHEN has_sql = 0 AND has_dashboard = 1 THEN employee_email END) AS dashboard_only_users,
    COUNT(DISTINCT CASE WHEN has_sql = 1 AND has_dashboard = 1 THEN employee_email END) AS both_users,
    
    -- Interaction counts
    SUM(sql_queries) AS total_sql_queries,
    SUM(dashboard_events) AS total_dashboard_events,
    SUM(dashboard_view_count) AS dashboard_view_count,
    SUM(dashboard_clone_count) AS dashboard_clone_count,
    SUM(dashboard_edit_count) AS dashboard_edit_count,
    SUM(sql_queries) + SUM(dashboard_events) AS total_interactions,
    
    -- Distinct resources
    SUM(unique_tables) AS unique_tables_queried,
    SUM(unique_dashboards) AS unique_dashboards_viewed,
    
    -- Collect dashboard counts for top dashboards
    FLATTEN(COLLECT_LIST(dashboard_counts)) AS all_dashboard_counts
    
  FROM combined_daily
  GROUP BY GROUPING SETS (
    (date),                                                      -- Overall
    (date, job_family_group),                                    -- Per job_family_group (Software/Hardware)
    (date, job_family_group, job_family),                        -- Per job_family (Firmware/Backend)
    (date, job_family_group, job_family, department),            -- Per department
    (date, job_family_group, job_family, department, employee_email)  -- Per employee
  )
  HAVING date BETWEEN DATE_SUB(DATE('{date_start}'), 364) AND DATE('{date_end}')
),

-- Calculate rolling metrics with nested structs by domain
rolling_metrics AS (
  SELECT
    date,
    user_hierarchy_id,
    all_dashboard_counts,
    
    -- Daily (current day values)
    NAMED_STRUCT(
      'users', NAMED_STRUCT(
        'total', total_active_users,
        'sql_users', sql_users,
        'dashboard_users', dashboard_users,
        'sql_only', sql_only_users,
        'dashboard_only', dashboard_only_users,
        'both', both_users
      ),
      'sql', NAMED_STRUCT(
        'queries', total_sql_queries,
        'unique_tables', unique_tables_queried
      ),
      'dashboard', NAMED_STRUCT(
        'events', total_dashboard_events,
        'views', dashboard_view_count,
        'clones', dashboard_clone_count,
        'edits', dashboard_edit_count,
        'unique_dashboards', unique_dashboards_viewed
      ),
      'combined', NAMED_STRUCT(
        'total_interactions', total_interactions,
        'active_days', CAST(1 AS LONG),
        'delta_users', CAST(0 AS LONG)
      )
    ) AS daily,
    
    -- Weekly (7-day rolling)
    NAMED_STRUCT(
      'users', NAMED_STRUCT(
        'total', SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'sql_users', SUM(sql_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'dashboard_users', SUM(dashboard_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'sql_only', SUM(sql_only_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'dashboard_only', SUM(dashboard_only_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'both', SUM(both_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      ),
      'sql', NAMED_STRUCT(
        'queries', SUM(total_sql_queries) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'unique_tables', SUM(unique_tables_queried) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      ),
      'dashboard', NAMED_STRUCT(
        'events', SUM(total_dashboard_events) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'views', SUM(dashboard_view_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'clones', SUM(dashboard_clone_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'edits', SUM(dashboard_edit_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'unique_dashboards', SUM(unique_dashboards_viewed) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
      ),
      'combined', NAMED_STRUCT(
        'total_interactions', SUM(total_interactions) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'active_days', COUNT(*) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        'delta_users', CAST(0 AS LONG)
      )
    ) AS weekly,
    
    -- Monthly (30-day rolling)
    NAMED_STRUCT(
      'users', NAMED_STRUCT(
        'total', SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'sql_users', SUM(sql_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'dashboard_users', SUM(dashboard_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'sql_only', SUM(sql_only_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'dashboard_only', SUM(dashboard_only_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'both', SUM(both_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ),
      'sql', NAMED_STRUCT(
        'queries', SUM(total_sql_queries) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'unique_tables', SUM(unique_tables_queried) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ),
      'dashboard', NAMED_STRUCT(
        'events', SUM(total_dashboard_events) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'views', SUM(dashboard_view_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'clones', SUM(dashboard_clone_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'edits', SUM(dashboard_edit_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'unique_dashboards', SUM(unique_dashboards_viewed) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ),
      'combined', NAMED_STRUCT(
        'total_interactions', SUM(total_interactions) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'active_days', COUNT(*) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        'delta_users', 
          SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) -
          LAG(SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 30) 
            OVER (PARTITION BY user_hierarchy_id ORDER BY date)
      )
    ) AS monthly,
    
    -- Quarterly (90-day rolling)
    NAMED_STRUCT(
      'users', NAMED_STRUCT(
        'total', SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'sql_users', SUM(sql_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'dashboard_users', SUM(dashboard_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'sql_only', SUM(sql_only_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'dashboard_only', SUM(dashboard_only_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'both', SUM(both_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
      ),
      'sql', NAMED_STRUCT(
        'queries', SUM(total_sql_queries) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'unique_tables', SUM(unique_tables_queried) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
      ),
      'dashboard', NAMED_STRUCT(
        'events', SUM(total_dashboard_events) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'views', SUM(dashboard_view_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'clones', SUM(dashboard_clone_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'edits', SUM(dashboard_edit_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'unique_dashboards', SUM(unique_dashboards_viewed) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
      ),
      'combined', NAMED_STRUCT(
        'total_interactions', SUM(total_interactions) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'active_days', COUNT(*) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        'delta_users',
          SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) -
          LAG(SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 90)
            OVER (PARTITION BY user_hierarchy_id ORDER BY date)
      )
    ) AS quarterly,
    
    -- Yearly (365-day rolling)
    NAMED_STRUCT(
      'users', NAMED_STRUCT(
        'total', SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'sql_users', SUM(sql_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'dashboard_users', SUM(dashboard_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'sql_only', SUM(sql_only_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'dashboard_only', SUM(dashboard_only_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'both', SUM(both_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
      ),
      'sql', NAMED_STRUCT(
        'queries', SUM(total_sql_queries) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'unique_tables', SUM(unique_tables_queried) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
      ),
      'dashboard', NAMED_STRUCT(
        'events', SUM(total_dashboard_events) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'views', SUM(dashboard_view_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'clones', SUM(dashboard_clone_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'edits', SUM(dashboard_edit_count) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'unique_dashboards', SUM(unique_dashboards_viewed) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
      ),
      'combined', NAMED_STRUCT(
        'total_interactions', SUM(total_interactions) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'active_days', COUNT(*) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW),
        'delta_users',
          SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) -
          LAG(SUM(total_active_users) OVER (PARTITION BY user_hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW), 365)
            OVER (PARTITION BY user_hierarchy_id ORDER BY date)
      )
    ) AS yearly
    
  FROM daily_agg
)

SELECT
  CAST(date AS STRING) AS date,
  user_hierarchy_id,
  daily,
  weekly,
  monthly,
  quarterly,
  yearly,
  
  -- Top 5 dashboards by events today
  TRANSFORM(
    SLICE(
      ARRAY_SORT(
        FILTER(all_dashboard_counts, x -> x IS NOT NULL AND x.dashboard_id IS NOT NULL),
        (left, right) -> CASE WHEN left.event_count > right.event_count THEN -1 
                              WHEN left.event_count < right.event_count THEN 1 
                              ELSE 0 END
      ),
      1, 5
    ),
    x -> x.dashboard_id
  ) AS top_dashboards_daily,
  
  -- Top 5 dashboards by events in 30 days (same as daily for simplicity - could be enhanced)
  TRANSFORM(
    SLICE(
      ARRAY_SORT(
        FILTER(all_dashboard_counts, x -> x IS NOT NULL AND x.dashboard_id IS NOT NULL),
        (left, right) -> CASE WHEN left.event_count > right.event_count THEN -1 
                              WHEN left.event_count < right.event_count THEN 1 
                              ELSE 0 END
      ),
      1, 5
    ),
    x -> x.dashboard_id
  ) AS top_dashboards_monthly

FROM rolling_metrics
WHERE date BETWEEN '{date_start}' AND '{date_end}'
ORDER BY date, user_hierarchy_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Unified platform usage combining SQL queries and dashboard activity. "
        "Hierarchy: job_family_group → job_family → department → employee. "
        "Join to dim_user_hierarchies on (date, user_hierarchy_id) for hierarchy details.",
        row_meaning="Each row represents aggregated platform usage metrics for a specific "
        "user_hierarchy_id (overall/job_family_group/job_family/department/employee) on a date.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TABLE_USER_QUERIES),
        AnyUpstream(ProductAnalyticsStaging.FCT_DASHBOARD_USAGE_DAILY),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_PLATFORM_USAGE_ROLLING.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_platform_usage_rolling(context: AssetExecutionContext) -> str:
    """
    Unified platform usage metrics combining SQL and dashboard activity.

    Key insights this enables:
    - Total active employees across ALL platform touchpoints
    - Employee segmentation: SQL-only vs dashboard-only vs power users (both)
    - Platform adoption trends (are more employees engaging with dashboards?)
    - Comparisons across job_family_group (Software vs Hardware)
    - Comparisons across job_family (Firmware vs Backend vs Frontend)
    - Department-level platform usage comparisons
    """
    return format_date_partition_query(
        query=QUERY,
        context=context,
        HIERARCHY_ID_SQL=HIERARCHY_ID_SQL,
    )
