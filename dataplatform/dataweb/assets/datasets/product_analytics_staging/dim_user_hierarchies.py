"""
User hierarchies dimension table (date-partitioned)

Defines aggregation populations for user/employee analysis by creating consistent hierarchy
identifiers. Uses job_family_group, job_family, department, and employee_email as dimension columns.

Date-partitioned to support backfilling and historical lookups. Join on date + user_hierarchy_id.

Two ways to select populations:

1. `grouping_columns` - Which dimension COLUMNS are in the grouping:
   - 'overall' = all employees aggregated
   - 'job_family_group' = Software vs Hardware vs other orgs
   - 'job_family_group.job_family' = Firmware vs Backend vs Frontend teams
   - 'job_family_group.job_family.department' = per-department rollup
   - 'job_family_group.job_family.department.employee_email' = per-employee grain

2. `hierarchy_label` - The actual VALUES for those columns:
   - 'overall' = everything
   - 'Software' = job_family_group level
   - 'Software.Firmware' = job_family level
   - 'Software.Firmware.100000 - Engineering' = department level
   - 'Software.Firmware.100000 - Engineering.alice@example.com' = specific employee

Example queries:
  -- All employee-level groupings
  WHERE grouping_columns = 'job_family_group.job_family.department.employee_email'

  -- Job family comparison (Firmware vs Backend)
  WHERE grouping_columns = 'job_family_group.job_family'

  -- Department-level rollup
  WHERE grouping_columns = 'job_family_group.job_family.department'

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
    Column,
    ColumnType,
    DataType,
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


# ============================================================================
# Hierarchy Configuration
# ============================================================================

# User-specific hierarchy ID - rename from generic HIERARCHY_ID_COLUMN
USER_HIERARCHY_ID_COLUMN = replace(
    HIERARCHY_ID_COLUMN,
    name="user_hierarchy_id",
    metadata=Metadata(comment="xxhash64 of user dimension values - FK to dim_user_hierarchies"),
)

# Dimension columns for GROUPING SETS (least to most granular)
# job_family_group → job_family → department → employee_email
HIERARCHY_COLUMNS = ["job_family_group", "job_family", "department", "employee_email"]

# Pre-built SQL for hierarchy columns
HIERARCHY_ID_SQL = build_grouping_id_sql(HIERARCHY_COLUMNS)
HIERARCHY_LABEL_SQL = build_grouping_label_sql(HIERARCHY_COLUMNS)
GROUPING_COLUMNS_SQL = build_grouping_columns_sql(HIERARCHY_COLUMNS)


# ============================================================================
# Reusable Column Definitions - import these in downstream tables
# ============================================================================

# Employee ID - references edw.silver.employee_hierarchy_active_vw
EMPLOYEE_ID_COLUMN = Column(
    name="employee_id",
    type=DataType.STRING,
    nullable=False,
    primary_key=True,
    metadata=Metadata(
        comment="Employee ID from edw.silver.employee_hierarchy_active_vw (joined on email)"
    ),
)

JOB_FAMILY_GROUP_COLUMN = Column(
    name="job_family_group",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="High-level org group: Software, Hardware, etc. (NULL for overall)"),
)

JOB_FAMILY_COLUMN = Column(
    name="job_family",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Team type: Firmware, Backend, Frontend, etc. (NULL for job_family_group/overall)"),
)

DEPARTMENT_COLUMN = Column(
    name="department",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Department code and name (NULL for job_family/overall levels)"),
)

EMPLOYEE_EMAIL_COLUMN = Column(
    name="employee_email",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Employee email address (NULL for department/job_family/overall levels)"),
)


# ============================================================================
# Population Count Columns - help understand hierarchy cardinality
# ============================================================================

EMPLOYEE_COUNT_COLUMN = Column(
    name="employee_count",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of employees in this hierarchy grouping"),
)

DISTINCT_JOB_FAMILY_GROUPS_COLUMN = Column(
    name="distinct_job_family_groups",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of distinct job family groups (NULL at employee level)"),
)

DISTINCT_JOB_FAMILIES_COLUMN = Column(
    name="distinct_job_families",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of distinct job families (NULL at employee/department level)"),
)

DISTINCT_DEPARTMENTS_COLUMN = Column(
    name="distinct_departments",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Count of distinct departments (NULL at employee level)"),
)


# ============================================================================
# Schema Definition
# ============================================================================

COLUMNS = [
    ColumnType.DATE,
    USER_HIERARCHY_ID_COLUMN,
    GROUPING_COLUMNS_COLUMN,
    HIERARCHY_LABEL_COLUMN,
    JOB_FAMILY_GROUP_COLUMN,
    JOB_FAMILY_COLUMN,
    DEPARTMENT_COLUMN,
    EMPLOYEE_EMAIL_COLUMN,
    # Population counts
    EMPLOYEE_COUNT_COLUMN,
    DISTINCT_JOB_FAMILY_GROUPS_COLUMN,
    DISTINCT_JOB_FAMILIES_COLUMN,
    DISTINCT_DEPARTMENTS_COLUMN,
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Get distinct employee dimension combinations from dashboard usage per date
-- Uses GROUPING SETS to create overall, job_family_group, job_family, department, and employee level hierarchies
-- Includes population counts for understanding hierarchy cardinality
WITH employee_dims AS (
  SELECT DISTINCT
    date,
    job_family_group,
    job_family,
    department,
    employee_email
  FROM {product_analytics_staging}.fct_dashboard_usage_daily
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
    AND employee_email IS NOT NULL
)

SELECT
  CAST(date AS STRING) AS date,
  {HIERARCHY_ID_SQL} AS user_hierarchy_id,
  {GROUPING_COLUMNS_SQL} AS grouping_columns,
  {HIERARCHY_LABEL_SQL} AS hierarchy_label,
  job_family_group,
  job_family,
  department,
  employee_email,
  -- Population counts help understand hierarchy cardinality
  COUNT(DISTINCT employee_email) AS employee_count,
  COUNT(DISTINCT job_family_group) AS distinct_job_family_groups,
  COUNT(DISTINCT job_family) AS distinct_job_families,
  COUNT(DISTINCT department) AS distinct_departments
FROM employee_dims
GROUP BY GROUPING SETS (
  (date),                                                    -- Overall
  (date, job_family_group),                                  -- Per job_family_group (Software/Hardware)
  (date, job_family_group, job_family),                      -- Per job_family (Firmware/Backend)
  (date, job_family_group, job_family, department),          -- Per department
  (date, job_family_group, job_family, department, employee_email)  -- Per employee
)
ORDER BY date, grouping_columns, hierarchy_label
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="User/employee hierarchies dimension - defines aggregation levels for engagement analytics. "
        "Hierarchy: job_family_group → job_family → department → employee.",
        row_meaning="Each row represents a unique hierarchy level (overall/job_family_group/job_family/department/employee) for a date.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_DASHBOARD_USAGE_DAILY),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_USER_HIERARCHIES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_user_hierarchies(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY, context,
        HIERARCHY_ID_SQL=HIERARCHY_ID_SQL,
        HIERARCHY_LABEL_SQL=HIERARCHY_LABEL_SQL,
        GROUPING_COLUMNS_SQL=GROUPING_COLUMNS_SQL,
    )
