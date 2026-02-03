"""
Rolling landing reliability metrics per table

Provides rolling window aggregations (7d, 30d) of partition landing reliability:
- Number of partitions landed
- Average/max landing latency
- SLA compliance rates (% meeting 1d/2d SLA)

Join to dim_tables on (date, table_id) for region/database/table names.

Grain: one row per date + table_id
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
    struct_with_comments,
    Column,
    ColumnType,
    DataType,
    Metadata,
)
from dataweb.assets.datasets.product_analytics_staging.dim_tables import (
    TABLE_ID_COLUMN,
)

# Define reliability metrics struct type
RELIABILITY_METRICS_STRUCT = struct_with_comments(
    (
        "partitions_landed",
        DataType.LONG,
        "Number of partitions that landed in this window",
    ),
    ("avg_latency_days", DataType.DOUBLE, "Average landing latency in days"),
    ("max_latency_days", DataType.INTEGER, "Maximum landing latency in days"),
    (
        "pct_meeting_1d_sla",
        DataType.DOUBLE,
        "Percentage of partitions meeting 1-day SLA (0-1)",
    ),
    (
        "pct_meeting_2d_sla",
        DataType.DOUBLE,
        "Percentage of partitions meeting 2-day SLA (0-1)",
    ),
)

# Schema definition - table_id references dim_tables
COLUMNS = [
    ColumnType.DATE,
    TABLE_ID_COLUMN,
    Column(
        name="daily",
        type=RELIABILITY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Daily reliability metrics"),
    ),
    Column(
        name="weekly",
        type=RELIABILITY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="7-day rolling window reliability metrics"),
    ),
    Column(
        name="monthly",
        type=RELIABILITY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="30-day rolling window reliability metrics"),
    ),
    Column(
        name="last_partition_landed",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Most recent partition date that has landed as of this date"
        ),
    ),
    Column(
        name="days_since_last_landing",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="Days since the last partition landed"),
    ),
    Column(
        name="freshness_status",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Freshness indicator: FRESH (<=2d), WARNING (3-7d), STALE (>7d). Add emojis in dashboard queries."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Generate date spine for output dates
WITH date_spine AS (
  SELECT DISTINCT date
  FROM {product_analytics_staging}.fct_partition_landings
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
),

-- Get all unique tables that have landed partitions (by table_id)
tables AS (
  SELECT DISTINCT table_id
  FROM {product_analytics_staging}.fct_partition_landings
  WHERE date BETWEEN DATE_SUB(DATE("{date_start}"), 29) AND DATE("{date_end}")
),

-- Cross join to get all date + table_id combinations
date_table_spine AS (
  SELECT d.date, t.table_id
  FROM date_spine d
  CROSS JOIN tables t
),

-- Join with fact data to get landings within window
windowed_data AS (
  SELECT
    s.date,
    s.table_id,
    f.date AS landing_date,
    f.partition_date,
    f.landing_latency_days,
    f.meets_1d_sla,
    f.meets_2d_sla
  FROM date_table_spine s
  LEFT JOIN {product_analytics_staging}.fct_partition_landings f
    ON s.table_id = f.table_id
    AND f.date BETWEEN DATE_SUB(s.date, 29) AND s.date
),

-- Calculate rolling metrics
rolling_metrics AS (
  SELECT
    date,
    table_id,
    
    -- Daily metrics (same day landings)
    COUNT(CASE WHEN landing_date = date THEN 1 END) AS daily_partitions,
    AVG(CASE WHEN landing_date = date THEN landing_latency_days END) AS daily_avg_latency,
    MAX(CASE WHEN landing_date = date THEN landing_latency_days END) AS daily_max_latency,
    AVG(CASE WHEN landing_date = date THEN CAST(meets_1d_sla AS DOUBLE) END) AS daily_pct_1d,
    AVG(CASE WHEN landing_date = date THEN CAST(meets_2d_sla AS DOUBLE) END) AS daily_pct_2d,
    
    -- 7-day rolling metrics
    COUNT(CASE WHEN landing_date BETWEEN DATE_SUB(date, 6) AND date THEN 1 END) AS weekly_partitions,
    AVG(CASE WHEN landing_date BETWEEN DATE_SUB(date, 6) AND date THEN landing_latency_days END) AS weekly_avg_latency,
    MAX(CASE WHEN landing_date BETWEEN DATE_SUB(date, 6) AND date THEN landing_latency_days END) AS weekly_max_latency,
    AVG(CASE WHEN landing_date BETWEEN DATE_SUB(date, 6) AND date THEN CAST(meets_1d_sla AS DOUBLE) END) AS weekly_pct_1d,
    AVG(CASE WHEN landing_date BETWEEN DATE_SUB(date, 6) AND date THEN CAST(meets_2d_sla AS DOUBLE) END) AS weekly_pct_2d,
    
    -- 30-day rolling metrics
    COUNT(CASE WHEN landing_date BETWEEN DATE_SUB(date, 29) AND date THEN 1 END) AS monthly_partitions,
    AVG(CASE WHEN landing_date BETWEEN DATE_SUB(date, 29) AND date THEN landing_latency_days END) AS monthly_avg_latency,
    MAX(CASE WHEN landing_date BETWEEN DATE_SUB(date, 29) AND date THEN landing_latency_days END) AS monthly_max_latency,
    AVG(CASE WHEN landing_date BETWEEN DATE_SUB(date, 29) AND date THEN CAST(meets_1d_sla AS DOUBLE) END) AS monthly_pct_1d,
    AVG(CASE WHEN landing_date BETWEEN DATE_SUB(date, 29) AND date THEN CAST(meets_2d_sla AS DOUBLE) END) AS monthly_pct_2d,
    
    -- Last partition landed (most recent partition_date, not landing_date)
    MAX(CASE WHEN landing_date <= date THEN partition_date END) AS last_partition_landed
    
  FROM windowed_data
  GROUP BY date, table_id
)

SELECT
  CAST(date AS STRING) AS date,
  table_id,
  
  NAMED_STRUCT(
    'partitions_landed', CAST(daily_partitions AS LONG),
    'avg_latency_days', daily_avg_latency,
    'max_latency_days', daily_max_latency,
    'pct_meeting_1d_sla', daily_pct_1d,
    'pct_meeting_2d_sla', daily_pct_2d
  ) AS daily,
  
  NAMED_STRUCT(
    'partitions_landed', CAST(weekly_partitions AS LONG),
    'avg_latency_days', weekly_avg_latency,
    'max_latency_days', weekly_max_latency,
    'pct_meeting_1d_sla', weekly_pct_1d,
    'pct_meeting_2d_sla', weekly_pct_2d
  ) AS weekly,
  
  NAMED_STRUCT(
    'partitions_landed', CAST(monthly_partitions AS LONG),
    'avg_latency_days', monthly_avg_latency,
    'max_latency_days', monthly_max_latency,
    'pct_meeting_1d_sla', monthly_pct_1d,
    'pct_meeting_2d_sla', monthly_pct_2d
  ) AS monthly,
  
  last_partition_landed,
  DATEDIFF(date, DATE(last_partition_landed)) AS days_since_last_landing,
  -- Store plain text status (add emojis in dashboard queries)
  CASE
    WHEN DATEDIFF(date, DATE(last_partition_landed)) <= 2 THEN 'FRESH'
    WHEN DATEDIFF(date, DATE(last_partition_landed)) <= 7 THEN 'WARNING'
    ELSE 'STALE'
  END AS freshness_status
  
FROM rolling_metrics
WHERE date BETWEEN "{date_start}" AND "{date_end}"
ORDER BY date, table_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Rolling window landing reliability metrics per table. Join to dim_tables for table names.",
        row_meaning="Each row represents aggregated landing reliability metrics (1d, 7d, 30d) for a specific table_id and date.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_PARTITION_LANDINGS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_LANDING_RELIABILITY_ROLLING.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_landing_reliability_rolling(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
