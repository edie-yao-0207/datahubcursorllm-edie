"""
Rolling window usage aggregations at multiple granularities

Computes rolling metrics (1d, 7d, 30d, 90d, 365d) for table usage including
unique users, query counts, and engagement metrics across different organizational dimensions.

Uses consistent cost_hierarchy_id from dim_cost_hierarchies for joining with cost tables.
Dimension details can be retrieved by joining to dim_cost_hierarchies on cost_hierarchy_id.
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
    struct_with_comments,
    Column,
    ColumnType,
    DataType,
    Metadata,
)
from dataweb.userpkgs.firmware.cost_hierarchy import HIERARCHY_ID_SQL
from dataweb.assets.datasets.product_analytics_staging.dim_cost_hierarchies import (
    COST_HIERARCHY_ID_COLUMN,
)

# Define usage metrics struct type
USAGE_METRICS_STRUCT = struct_with_comments(
    ("unique_users", DataType.LONG, "Distinct count of users in window"),
    ("total_queries", DataType.LONG, "Total query count in window"),
    ("unique_tables", DataType.LONG, "Distinct count of tables queried in window"),
    ("avg_queries_per_user", DataType.DOUBLE, "Average queries per user"),
    ("avg_queries_per_table", DataType.DOUBLE, "Average queries per table"),
    (
        "delta_unique_users",
        DataType.LONG,
        "Change in unique users from previous period",
    ),
)

# Schema definition - just date and cost_cost_hierarchy_id for joining
COLUMNS = [
    ColumnType.DATE,
    COST_HIERARCHY_ID_COLUMN,
    Column(
        name="daily",
        type=USAGE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Daily usage metrics"),
    ),
    Column(
        name="weekly",
        type=USAGE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="7-day rolling usage metrics"),
    ),
    Column(
        name="monthly",
        type=USAGE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="30-day rolling usage metrics"),
    ),
    Column(
        name="quarterly",
        type=USAGE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="90-day rolling usage metrics"),
    ),
    Column(
        name="yearly",
        type=USAGE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="365-day rolling usage metrics"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Get service metadata from dim_billing_services for consistent dimensions
WITH service_metadata AS (
  SELECT DISTINCT
    date,
    service,
    region,
    team,
    product_group,
    dataplatform_feature,
    dagster_job.database AS database,
    dagster_job.table AS table
  FROM {product_analytics_staging}.dim_billing_services
  WHERE date BETWEEN DATE_SUB(DATE('{date_start}'), 364) AND DATE('{date_end}')
    AND dagster_job.database IS NOT NULL
    AND dagster_job.table IS NOT NULL
),

raw_queries AS (
  SELECT
    q.date,
    q.email,
    q.database_name AS database,
    q.table_name AS table
  FROM auditlog.databricks_tables_queried q
  WHERE q.date BETWEEN DATE_SUB(DATE('{date_start}'), 364) AND DATE('{date_end}')
    AND q.email IS NOT NULL
    AND q.database_name IS NOT NULL
    AND q.table_name IS NOT NULL
),

-- Enrich queries with service dimensions (join on date + database + table)
enriched_queries AS (
  SELECT
    q.date,
    q.email,
    q.database,
    q.table,
    s.service,
    s.region,
    s.team,
    s.product_group,
    s.dataplatform_feature
  FROM raw_queries q
  LEFT JOIN service_metadata s
    USING (date, database, table)
),

-- Date spine for output dates
date_spine AS (
  SELECT DISTINCT date
  FROM enriched_queries
  WHERE date BETWEEN DATE('{date_start}') AND DATE('{date_end}')
),

-- Pre-join: each output date to all raw data within its 365-day window
-- This is the expensive join, but done ONCE before GROUPING SETS
date_window_data AS (
  SELECT
    ds.date AS target_date,
    e.date AS source_date,
    e.email,
    e.database,
    e.table,
    e.region,
    e.team,
    e.product_group,
    e.dataplatform_feature,
    e.service
  FROM date_spine ds
  JOIN enriched_queries e
    ON e.date BETWEEN DATE_SUB(ds.date, 364) AND ds.date
),

-- Apply GROUPING SETS on pre-joined data
-- GROUPING SETS naturally handles dimension filtering: rows only contribute to their matching groups
rolling_metrics AS (
  SELECT
    target_date AS date,
    {HIERARCHY_ID_SQL} AS cost_hierarchy_id,
    
    -- Daily metrics (1-day window)
    COUNT(DISTINCT CASE WHEN source_date = target_date THEN email END) AS daily_unique_users,
    COUNT(CASE WHEN source_date = target_date THEN 1 END) AS daily_total_queries,
    COUNT(DISTINCT CASE WHEN source_date = target_date THEN CONCAT(database, '.', table) END) AS daily_unique_tables,
    
    -- 7-day rolling (true distinct counts over window)
    COUNT(DISTINCT CASE WHEN source_date BETWEEN DATE_SUB(target_date, 6) AND target_date THEN email END) AS rolling_7d_unique_users,
    COUNT(CASE WHEN source_date BETWEEN DATE_SUB(target_date, 6) AND target_date THEN 1 END) AS rolling_7d_total_queries,
    COUNT(DISTINCT CASE WHEN source_date BETWEEN DATE_SUB(target_date, 6) AND target_date THEN CONCAT(database, '.', table) END) AS rolling_7d_unique_tables,
    
    -- 30-day rolling (true distinct counts over window)
    COUNT(DISTINCT CASE WHEN source_date BETWEEN DATE_SUB(target_date, 29) AND target_date THEN email END) AS rolling_30d_unique_users,
    COUNT(CASE WHEN source_date BETWEEN DATE_SUB(target_date, 29) AND target_date THEN 1 END) AS rolling_30d_total_queries,
    COUNT(DISTINCT CASE WHEN source_date BETWEEN DATE_SUB(target_date, 29) AND target_date THEN CONCAT(database, '.', table) END) AS rolling_30d_unique_tables,
    
    -- 90-day rolling (true distinct counts over window)
    COUNT(DISTINCT CASE WHEN source_date BETWEEN DATE_SUB(target_date, 89) AND target_date THEN email END) AS rolling_90d_unique_users,
    COUNT(CASE WHEN source_date BETWEEN DATE_SUB(target_date, 89) AND target_date THEN 1 END) AS rolling_90d_total_queries,
    COUNT(DISTINCT CASE WHEN source_date BETWEEN DATE_SUB(target_date, 89) AND target_date THEN CONCAT(database, '.', table) END) AS rolling_90d_unique_tables,
    
    -- 365-day rolling (true distinct counts over window)
    COUNT(DISTINCT email) AS rolling_365d_unique_users,
    COUNT(*) AS rolling_365d_total_queries,
    COUNT(DISTINCT CONCAT(database, '.', table)) AS rolling_365d_unique_tables
    
  FROM date_window_data
  GROUP BY
    GROUPING SETS (
      -- Overall
      (target_date),
      -- Region level
      (target_date, region),
      -- Team level
      (target_date, region, team),
      -- Product group level
      (target_date, region, team, product_group),
      -- DataPlatform feature level
      (target_date, region, team, product_group, dataplatform_feature),
      -- Service level (most granular)
      (target_date, region, team, product_group, dataplatform_feature, service),
      -- Common groupings for dashboard filters
      (target_date, team),
      (target_date, team, product_group),
      (target_date, team, product_group, dataplatform_feature)
    )
),

with_deltas AS (
  SELECT
    cost_hierarchy_id,
    date,
    
    -- Daily metrics
    daily_unique_users AS rolling_1d_unique_users,
    daily_total_queries AS rolling_1d_total_queries,
    daily_unique_tables AS rolling_1d_unique_tables,
    
    -- Rolling windows
    rolling_7d_unique_users,
    rolling_7d_total_queries,
    rolling_7d_unique_tables,
    rolling_30d_unique_users,
    rolling_30d_total_queries,
    rolling_30d_unique_tables,
    rolling_90d_unique_users,
    rolling_90d_total_queries,
    rolling_90d_unique_tables,
    rolling_365d_unique_users,
    rolling_365d_total_queries,
    rolling_365d_unique_tables,

    -- Calculate deltas using LAG
    (daily_unique_users - LAG(daily_unique_users, 1) OVER (PARTITION BY cost_hierarchy_id ORDER BY date)) AS delta_1d_unique_users,
    (rolling_7d_unique_users - LAG(rolling_7d_unique_users, 1) OVER (PARTITION BY cost_hierarchy_id ORDER BY date)) AS delta_7d_unique_users,
    (rolling_30d_unique_users - LAG(rolling_30d_unique_users, 1) OVER (PARTITION BY cost_hierarchy_id ORDER BY date)) AS delta_30d_unique_users,
    (rolling_90d_unique_users - LAG(rolling_90d_unique_users, 1) OVER (PARTITION BY cost_hierarchy_id ORDER BY date)) AS delta_90d_unique_users,
    (rolling_365d_unique_users - LAG(rolling_365d_unique_users, 1) OVER (PARTITION BY cost_hierarchy_id ORDER BY date)) AS delta_365d_unique_users

  FROM rolling_metrics
)

SELECT
  CAST(date AS STRING) AS date,
  cost_hierarchy_id,
  
  NAMED_STRUCT(
    'unique_users', rolling_1d_unique_users, 
    'total_queries', rolling_1d_total_queries,
    'unique_tables', rolling_1d_unique_tables,
    'avg_queries_per_user', CASE WHEN rolling_1d_unique_users > 0 THEN rolling_1d_total_queries / rolling_1d_unique_users ELSE 0 END,
    'avg_queries_per_table', CASE WHEN rolling_1d_unique_tables > 0 THEN rolling_1d_total_queries / rolling_1d_unique_tables ELSE 0 END,
    'delta_unique_users', delta_1d_unique_users
  ) AS daily,
  
  NAMED_STRUCT(
    'unique_users', rolling_7d_unique_users, 
    'total_queries', rolling_7d_total_queries,
    'unique_tables', rolling_7d_unique_tables,
    'avg_queries_per_user', CASE WHEN rolling_7d_unique_users > 0 THEN rolling_7d_total_queries / rolling_7d_unique_users ELSE 0 END,
    'avg_queries_per_table', CASE WHEN rolling_7d_unique_tables > 0 THEN rolling_7d_total_queries / rolling_7d_unique_tables ELSE 0 END,
    'delta_unique_users', delta_7d_unique_users
  ) AS weekly,
  
  NAMED_STRUCT(
    'unique_users', rolling_30d_unique_users, 
    'total_queries', rolling_30d_total_queries,
    'unique_tables', rolling_30d_unique_tables,
    'avg_queries_per_user', CASE WHEN rolling_30d_unique_users > 0 THEN rolling_30d_total_queries / rolling_30d_unique_users ELSE 0 END,
    'avg_queries_per_table', CASE WHEN rolling_30d_unique_tables > 0 THEN rolling_30d_total_queries / rolling_30d_unique_tables ELSE 0 END,
    'delta_unique_users', delta_30d_unique_users
  ) AS monthly,
  
  NAMED_STRUCT(
    'unique_users', rolling_90d_unique_users, 
    'total_queries', rolling_90d_total_queries,
    'unique_tables', rolling_90d_unique_tables,
    'avg_queries_per_user', CASE WHEN rolling_90d_unique_users > 0 THEN rolling_90d_total_queries / rolling_90d_unique_users ELSE 0 END,
    'avg_queries_per_table', CASE WHEN rolling_90d_unique_tables > 0 THEN rolling_90d_total_queries / rolling_90d_unique_tables ELSE 0 END,
    'delta_unique_users', delta_90d_unique_users
  ) AS quarterly,
  
  NAMED_STRUCT(
    'unique_users', rolling_365d_unique_users, 
    'total_queries', rolling_365d_total_queries,
    'unique_tables', rolling_365d_unique_tables,
    'avg_queries_per_user', CASE WHEN rolling_365d_unique_users > 0 THEN rolling_365d_total_queries / rolling_365d_unique_users ELSE 0 END,
    'avg_queries_per_table', CASE WHEN rolling_365d_unique_tables > 0 THEN rolling_365d_total_queries / rolling_365d_unique_tables ELSE 0 END,
    'delta_unique_users', delta_365d_unique_users
  ) AS yearly

FROM with_deltas
WHERE date BETWEEN "{date_start}" AND "{date_end}"
ORDER BY date, cost_hierarchy_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Rolling window usage aggregations with cost_hierarchy_id for joining to costs and dim_cost_hierarchies.",
        row_meaning="Each row represents aggregated usage metrics over multiple time horizons (1d, 7d, 30d, 90d, 365d) for a specific hierarchy and date.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Auditlog.DATABRICKS_TABLES_QUERIED),
        AnyUpstream(ProductAnalyticsStaging.DIM_BILLING_SERVICES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TABLE_USAGE_ROLLING.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_table_usage_rolling(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        query=QUERY,
        context=context,
        HIERARCHY_ID_SQL=HIERARCHY_ID_SQL,
    )
