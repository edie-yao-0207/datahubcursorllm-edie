"""
Daily per-employee, per-dashboard event aggregations

Aggregates raw dashboard events from auditlog.fct_databricks_dashboard_events into
daily summaries with action type categorization and employee metadata enrichment.

Uses employee_id from edw.silver.employee_hierarchy_active_vw (joined on email) for consistent
user identification and employee metadata (job_family_group, job_family, department).

Join to dim_dashboards on (date, dashboard_id) for workspace_id and raw_dashboard_id.

Grain: one row per date + employee_id + dashboard_id (hash)
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
    array_of,
)

# Import from dimension tables
from dataweb.assets.datasets.product_analytics_staging.dim_dashboards import (
    DASHBOARD_ID_COLUMN,
    DASHBOARD_ID_SQL,
)
from dataweb.assets.datasets.product_analytics_staging.dim_user_hierarchies import (
    EMPLOYEE_ID_COLUMN,
)


# Schema definition - employee_id references employee_hierarchy, dashboard_id references dim_dashboards
COLUMNS = [
    ColumnType.DATE,
    EMPLOYEE_ID_COLUMN,
    DASHBOARD_ID_COLUMN,
    Column(
        name="employee_email",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Employee email address"),
    ),
    Column(
        name="job_family_group",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="High-level org group: Software, Hardware, etc."),
    ),
    Column(
        name="job_family",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Team type: Firmware, Backend, Frontend, etc."),
    ),
    Column(
        name="department",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Department code and name"),
    ),
    Column(
        name="total_events",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total number of events for this employee+dashboard+date"
        ),
    ),
    Column(
        name="view_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Number of view actions"),
    ),
    Column(
        name="clone_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Number of clone actions"),
    ),
    Column(
        name="edit_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Number of edit/update actions"),
    ),
    Column(
        name="other_action_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Number of other actions (not view/clone/edit)"),
    ),
    Column(
        name="action_types",
        type=array_of(DataType.STRING),
        nullable=True,
        metadata=Metadata(comment="Array of distinct action names performed"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = r"""
WITH events AS (
  SELECT 
    date,
    user_email,
    workspace_id,
    dashboard_id,
    action_name
  FROM auditlog.fct_databricks_dashboard_events
  WHERE date BETWEEN '{date_start}' AND '{date_end}'
    AND user_email IS NOT NULL
    AND user_email LIKE '%@%'
),

-- Get employee metadata from employee hierarchy view
employee_lookup AS (
  SELECT
    employee_id,
    employee_email,
    job_family_group,
    job_family,
    department
  FROM edw.silver.employee_hierarchy_active_vw
  WHERE is_active = TRUE
)

SELECT
  CAST(ev.date AS STRING) AS date,
  e.employee_id,
  {DASHBOARD_ID_SQL} AS dashboard_id,
  e.employee_email,
  e.job_family_group,
  e.job_family,
  e.department,
  COUNT(*) AS total_events,
  SUM(CASE WHEN LOWER(action_name) LIKE '%view%' THEN 1 ELSE 0 END) AS view_count,
  SUM(CASE WHEN LOWER(action_name) LIKE '%clone%' THEN 1 ELSE 0 END) AS clone_count,
  SUM(CASE WHEN LOWER(action_name) LIKE '%edit%' OR LOWER(action_name) LIKE '%update%' THEN 1 ELSE 0 END) AS edit_count,
  SUM(CASE WHEN LOWER(action_name) NOT LIKE '%view%' 
           AND LOWER(action_name) NOT LIKE '%clone%'
           AND LOWER(action_name) NOT LIKE '%edit%'
           AND LOWER(action_name) NOT LIKE '%update%' THEN 1 ELSE 0 END) AS other_action_count,
  COLLECT_SET(action_name) AS action_types
FROM events ev
INNER JOIN employee_lookup e ON ev.user_email = e.employee_email
GROUP BY ev.date, e.employee_id, e.employee_email, e.job_family_group, e.job_family, e.department, ev.workspace_id, ev.dashboard_id
ORDER BY ev.date, e.employee_id, dashboard_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily dashboard usage metrics per employee and dashboard. Uses employee_id from edw.silver.employee_hierarchy_active_vw.",
        row_meaning="Each row represents one employee's activity on one dashboard (by hash ID) for one date.",
        table_type=TableType.TRANSACTIONAL_FACT,
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
        asset_name=ProductAnalyticsStaging.FCT_DASHBOARD_USAGE_DAILY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_dashboard_usage_daily(context: AssetExecutionContext) -> str:
    """
    Aggregate raw dashboard events into daily per-employee, per-dashboard summaries.

    This table provides the foundation for dashboard usage analytics by:
    - Aggregating raw events from auditlog.fct_databricks_dashboard_events
    - Categorizing actions into view/clone/edit/other buckets
    - Enriching with employee metadata from edw.silver.employee_hierarchy_active_vw
    - Filtering out non-email system accounts
    - Using dashboard_id hash (join to dim_dashboards for workspace/raw ID)

    Use cases:
    - Power user identification by dashboard
    - Action pattern analysis (viewing vs editing behavior)
    - Department-level adoption tracking
    - User engagement scoring
    - Dashboard popularity metrics
    """

    return format_date_partition_query(
        query=QUERY,
        context=context,
        DASHBOARD_ID_SQL=DASHBOARD_ID_SQL,
    )
