"""
Table user queries - atomic fact table for SQL usage (pure star schema)

Grain: one row per (date, table_id, employee_id) with query count.

Pure star schema design - fact table contains only:
- Foreign keys to dimension tables
- Measures

Joins:
- table_id → dim_tables (for database, table, team)
- employee_id → edw.silver.employee_hierarchy_active_vw (for email, department, job_family)

Only includes queries to managed tables (those in dim_tables) by employees in HR system.
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
)
from dataweb.assets.datasets.product_analytics_staging.dim_tables import (
    TABLE_ID_COLUMN,
)
from dataclasses import replace


# ============================================================================
# Schema definition - Pure Star Schema (minimal)
# ============================================================================

# Reuse table_id column from dim_tables, mark as primary key
TABLE_ID_FK_COLUMN = replace(
    TABLE_ID_COLUMN,
    primary_key=True,
    metadata=Metadata(comment="FK to dim_tables - join for database, table, team"),
)

COLUMNS = [
    ColumnType.DATE,
    # Table dimension FK
    TABLE_ID_FK_COLUMN,
    # Employee dimension FK
    Column(
        name="employee_id",
        type=DataType.LONG,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="FK to edw.silver.employee_hierarchy_active_vw - join for email, department, job_family"),
    ),
    # Measure
    Column(
        name="query_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Number of queries by this employee on this table on this date"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = r"""
-- Atomic fact table: pure star schema
-- Grain: (date, table_id, employee_id)
-- Only FKs and measures - join to dimensions for attributes

WITH query_counts AS (
  -- Aggregate queries per user per table per day
  SELECT
    q.date,
    q.database_name AS database,
    q.table_name AS `table`,
    q.email AS employee_email,
    COUNT(*) AS query_count
  FROM auditlog.databricks_tables_queried q
  WHERE q.date BETWEEN '{date_start}' AND '{date_end}'
    AND q.email IS NOT NULL
    AND q.email LIKE '%@%'
    AND q.database_name IS NOT NULL
    AND q.table_name IS NOT NULL
  GROUP BY q.date, q.database_name, q.table_name, q.email
),

-- Get table_id from dim_tables (only managed tables)
-- Join on date directly so each day's fact uses that day's dimension snapshot
table_lookup AS (
  SELECT
    date,
    database,
    `table`,
    table_id
  FROM {product_analytics_staging}.dim_tables
  WHERE date BETWEEN '{date_start}' AND '{date_end}'
),

-- Get employee_id from employee hierarchy (only known employees)
employee_lookup AS (
  SELECT
    employee_id,
    employee_email
  FROM edw.silver.employee_hierarchy_active_vw
  WHERE is_active = TRUE
)

SELECT
  CAST(q.date AS STRING) AS date,
  t.table_id,
  CAST(e.employee_id AS BIGINT) AS employee_id,
  q.query_count
FROM query_counts q
INNER JOIN table_lookup t USING (date, database, `table`)  -- Only managed tables, joined on date
INNER JOIN employee_lookup e ON q.employee_email = e.employee_email  -- Only known employees
ORDER BY q.date, t.table_id, e.employee_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Atomic fact table for SQL usage (pure star schema). "
        "Join to dim_tables for table attrs, employee_hierarchy_active_vw for employee attrs. "
        "Only includes queries to managed tables by known employees.",
        row_meaning="Each row represents queries from one employee to one managed table on one date.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Auditlog.DATABRICKS_TABLES_QUERIED),
        AnyUpstream(ProductAnalyticsStaging.DIM_TABLES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TABLE_USER_QUERIES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_table_user_queries(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        query=QUERY,
        context=context,
    )
