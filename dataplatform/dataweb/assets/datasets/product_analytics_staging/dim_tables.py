"""
Table dimension - canonical registry of tables by table_id

Provides a single source of truth for table metadata (region, database, table).
Derived from dim_billing_services dagster jobs.

This is the dimension table that owns:
- TABLE_ID_COLUMN, TABLE_ID_SQL - hash key for joining
- REGION_COLUMN, DATABASE_COLUMN, TABLE_COLUMN - table attributes

Downstream fact/aggregate tables should import these columns from here.

Grain: one row per unique table (region + database + table)
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
from dataweb.userpkgs.firmware.hash_utils import hash_id_sql, hash_id_sql_raw


# ============================================================================
# Table ID Hash Key - owned by this dimension table
# ============================================================================

# Columns used to generate table_id hash
TABLE_ID_HASH_COLUMNS = ["region", "database", "`table`"]
TABLE_ID_SQL = hash_id_sql(TABLE_ID_HASH_COLUMNS)

# For dim_billing_services where columns come from dagster_job struct
DAGSTER_TABLE_ID_COLUMNS = [
    "COALESCE(region, '')",
    "COALESCE(dagster_job.database, '')",
    "COALESCE(dagster_job.table, '')",
]
DAGSTER_TABLE_ID_SQL = hash_id_sql_raw(DAGSTER_TABLE_ID_COLUMNS)


# ============================================================================
# Reusable Column Definitions - import these in downstream tables
# ============================================================================

TABLE_ID_COLUMN = Column(
    name="table_id",
    type=DataType.LONG,
    nullable=False,
    primary_key=True,
    metadata=Metadata(
        comment="xxhash64(region|database|table) - FK to dim_tables"
    ),
)

REGION_COLUMN = Column(
    name="region",
    type=DataType.STRING,
    nullable=False,
    metadata=Metadata(comment="AWS region (us-west-2, eu-west-1, ca-central-1)"),
)

DATABASE_COLUMN = Column(
    name="database",
    type=DataType.STRING,
    nullable=False,
    metadata=Metadata(comment="Database name"),
)

TABLE_COLUMN = Column(
    name="table",
    type=DataType.STRING,
    nullable=False,
    metadata=Metadata(comment="Table name"),
)

TEAM_COLUMN = Column(
    name="team",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Owning team from billing (may be NULL if not in billing)"),
)


# ============================================================================
# Schema definition
# ============================================================================

COLUMNS = [
    ColumnType.DATE,
    TABLE_ID_COLUMN,
    REGION_COLUMN,
    DATABASE_COLUMN,
    TABLE_COLUMN,
    TEAM_COLUMN,
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Extract distinct tables from billing services (dagster jobs)
-- table_id is hash of (region, database, table) for efficient joins
-- Use MAX_BY(team, date) to pick the team from the most recent record
SELECT
  CAST(date AS STRING) AS date,
  {DAGSTER_TABLE_ID_SQL} AS table_id,
  region,
  dagster_job.database AS database,
  dagster_job.table AS `table`,
  MAX_BY(team, date) AS team
FROM {product_analytics_staging}.dim_billing_services
WHERE date BETWEEN "{date_start}" AND "{date_end}"
  AND dagster_job IS NOT NULL
  AND dagster_job.database IS NOT NULL
  AND dagster_job.table IS NOT NULL
GROUP BY date, region, dagster_job.database, dagster_job.table
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Table dimension - canonical registry of tables with table_id for joining fact tables.",
        row_meaning="Each row represents a unique table (region + database + table) with its owning team.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_BILLING_SERVICES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_TABLES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_tables(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context, DAGSTER_TABLE_ID_SQL=DAGSTER_TABLE_ID_SQL)
