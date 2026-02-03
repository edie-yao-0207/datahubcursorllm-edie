"""
Cost anomaly detection using z-score analysis

Detects anomalous cost increases by comparing rolling costs against historical baselines
using 3-sigma thresholds across multiple time horizons.

Uses cost_hierarchy_id for efficient joining - get dimension details from dim_cost_hierarchies.
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
from dataweb.assets.datasets.product_analytics_staging.dim_cost_hierarchies import (
    COST_HIERARCHY_ID_COLUMN,
)

# Define struct types for anomaly metrics
BASELINE_METRICS_STRUCT = struct_with_comments(
    (
        "mean",
        DataType.DOUBLE,
        "Baseline mean cost (window varies: 30d for daily/weekly, 90d for monthly, 180d for quarterly, 365d for yearly)",
    ),
    ("stddev", DataType.DOUBLE, "Baseline standard deviation"),
)

ANOMALY_METRICS_STRUCT = struct_with_comments(
    ("cost", DataType.DOUBLE, "Current cost value"),
    ("z_score", DataType.DOUBLE, "Z-score vs baseline"),
    (
        "percent_change",
        DataType.DOUBLE,
        "Proportional change from baseline mean (0-1 scale, e.g., 0.5 = 50% increase)",
    ),
    (
        "status",
        DataType.STRING,
        "Anomaly status: normal, warning, anomalous_increase, anomalous_decrease, critical_increase, critical_decrease, insufficient_history",
    ),
)

# Schema definition - just date and cost_cost_hierarchy_id for joining
COLUMNS = [
    ColumnType.DATE,
    COST_HIERARCHY_ID_COLUMN,
    Column(
        name="daily",
        type=ANOMALY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Daily anomaly detection metrics"),
    ),
    Column(
        name="daily_baseline",
        type=BASELINE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Daily baseline statistics"),
    ),
    Column(
        name="weekly",
        type=ANOMALY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="7-day rolling window anomaly metrics"),
    ),
    Column(
        name="weekly_baseline",
        type=BASELINE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Weekly baseline statistics"),
    ),
    Column(
        name="monthly",
        type=ANOMALY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="30-day rolling window anomaly metrics"),
    ),
    Column(
        name="monthly_baseline",
        type=BASELINE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Monthly baseline statistics"),
    ),
    Column(
        name="quarterly",
        type=ANOMALY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="90-day rolling window anomaly metrics"),
    ),
    Column(
        name="quarterly_baseline",
        type=BASELINE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Quarterly baseline statistics"),
    ),
    Column(
        name="yearly",
        type=ANOMALY_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="365-day rolling window anomaly metrics"),
    ),
    Column(
        name="yearly_baseline",
        type=BASELINE_METRICS_STRUCT,
        nullable=True,
        metadata=Metadata(comment="Yearly baseline statistics"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
-- Fetch 365 days of historical data for baseline calculation
-- Uses cost_hierarchy_id for efficient window partitioning
WITH base AS (
  SELECT
    date,
    cost_hierarchy_id,
    daily.cost AS daily_cost,
    weekly.cost AS weekly_cost,
    monthly.cost AS monthly_cost,
    quarterly.cost AS quarterly_cost,
    yearly.cost AS yearly_cost
  FROM {product_analytics_staging}.agg_costs_rolling
  WHERE 
    date BETWEEN DATE_SUB(DATE('{date_start}'), 365) AND DATE('{date_end}')
),

-- Calculate rolling baselines using cost_hierarchy_id for partitioning
-- Excludes current day (1 PRECEDING) to avoid look-ahead bias
scored AS (
  SELECT
    date,
    cost_hierarchy_id,
    daily_cost,
    weekly_cost,
    monthly_cost,
    quarterly_cost,
    yearly_cost,

    -- Daily baseline (30 days)
    AVG(daily_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) AS baseline_daily_mean,
    STDDEV_POP(daily_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) AS baseline_daily_stddev,

    -- Weekly baseline (30 days)
    AVG(weekly_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) AS baseline_weekly_mean,
    STDDEV_POP(weekly_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) AS baseline_weekly_stddev,

    -- Monthly baseline (90 days for more stable trend)
    AVG(monthly_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING
    ) AS baseline_monthly_mean,
    STDDEV_POP(monthly_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING
    ) AS baseline_monthly_stddev,

    -- Quarterly baseline (180 days for longer trends)
    AVG(quarterly_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING
    ) AS baseline_quarterly_mean,
    STDDEV_POP(quarterly_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 180 PRECEDING AND 1 PRECEDING
    ) AS baseline_quarterly_stddev,

    -- Yearly baseline (365 days for annual trends)
    AVG(yearly_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 365 PRECEDING AND 1 PRECEDING
    ) AS baseline_yearly_mean,
    STDDEV_POP(yearly_cost) OVER (
      PARTITION BY cost_hierarchy_id
      ORDER BY date
      ROWS BETWEEN 365 PRECEDING AND 1 PRECEDING
    ) AS baseline_yearly_stddev
  FROM base
)

SELECT
  date,
  cost_hierarchy_id,

  -- Daily anomaly metrics with severity thresholds
  -- z_score >= 2: warning, >= 3: anomalous, >= 5: critical (both increases and decreases)
  NAMED_STRUCT(
    'cost', daily_cost,
    'z_score',
    CASE
      WHEN baseline_daily_stddev IS NOT NULL AND baseline_daily_stddev > 0
      THEN (daily_cost - baseline_daily_mean) / baseline_daily_stddev
      ELSE NULL
    END,
    'percent_change',
    CASE
      WHEN baseline_daily_mean IS NOT NULL AND baseline_daily_mean > 0
      THEN (daily_cost - baseline_daily_mean) / baseline_daily_mean
      ELSE NULL
    END,
    'status',
    CASE
      WHEN baseline_daily_mean IS NULL OR baseline_daily_stddev IS NULL
        THEN 'insufficient_history'
      WHEN baseline_daily_stddev > 0 AND (daily_cost - baseline_daily_mean) / baseline_daily_stddev >= 5
        THEN 'critical_increase'
      WHEN baseline_daily_stddev > 0 AND (daily_cost - baseline_daily_mean) / baseline_daily_stddev <= -5
        THEN 'critical_decrease'
      WHEN baseline_daily_stddev > 0 AND (daily_cost - baseline_daily_mean) / baseline_daily_stddev >= 3
        THEN 'anomalous_increase'
      WHEN baseline_daily_stddev > 0 AND (daily_cost - baseline_daily_mean) / baseline_daily_stddev <= -3
        THEN 'anomalous_decrease'
      WHEN baseline_daily_stddev > 0 AND (daily_cost - baseline_daily_mean) / baseline_daily_stddev >= 2
        THEN 'warning'
      ELSE 'normal'
    END
  ) AS daily,

  NAMED_STRUCT(
    'mean', baseline_daily_mean,
    'stddev', baseline_daily_stddev
  ) AS daily_baseline,

  -- Weekly anomaly metrics
  NAMED_STRUCT(
    'cost', weekly_cost,
    'z_score',
    CASE
      WHEN baseline_weekly_stddev IS NOT NULL AND baseline_weekly_stddev > 0
      THEN (weekly_cost - baseline_weekly_mean) / baseline_weekly_stddev
      ELSE NULL
    END,
    'percent_change',
    CASE
      WHEN baseline_weekly_mean IS NOT NULL AND baseline_weekly_mean > 0
      THEN (weekly_cost - baseline_weekly_mean) / baseline_weekly_mean
      ELSE NULL
    END,
    'status',
    CASE
      WHEN baseline_weekly_mean IS NULL OR baseline_weekly_stddev IS NULL
        THEN 'insufficient_history'
      WHEN baseline_weekly_stddev > 0 AND (weekly_cost - baseline_weekly_mean) / baseline_weekly_stddev >= 5
        THEN 'critical_increase'
      WHEN baseline_weekly_stddev > 0 AND (weekly_cost - baseline_weekly_mean) / baseline_weekly_stddev <= -5
        THEN 'critical_decrease'
      WHEN baseline_weekly_stddev > 0 AND (weekly_cost - baseline_weekly_mean) / baseline_weekly_stddev >= 3
        THEN 'anomalous_increase'
      WHEN baseline_weekly_stddev > 0 AND (weekly_cost - baseline_weekly_mean) / baseline_weekly_stddev <= -3
        THEN 'anomalous_decrease'
      WHEN baseline_weekly_stddev > 0 AND (weekly_cost - baseline_weekly_mean) / baseline_weekly_stddev >= 2
        THEN 'warning'
      ELSE 'normal'
    END
  ) AS weekly,

  NAMED_STRUCT(
    'mean', baseline_weekly_mean,
    'stddev', baseline_weekly_stddev
  ) AS weekly_baseline,

  -- Monthly anomaly metrics
  NAMED_STRUCT(
    'cost', monthly_cost,
    'z_score',
    CASE
      WHEN baseline_monthly_stddev IS NOT NULL AND baseline_monthly_stddev > 0
      THEN (monthly_cost - baseline_monthly_mean) / baseline_monthly_stddev
      ELSE NULL
    END,
    'percent_change',
    CASE
      WHEN baseline_monthly_mean IS NOT NULL AND baseline_monthly_mean > 0
      THEN (monthly_cost - baseline_monthly_mean) / baseline_monthly_mean
      ELSE NULL
    END,
    'status',
    CASE
      WHEN baseline_monthly_mean IS NULL OR baseline_monthly_stddev IS NULL
        THEN 'insufficient_history'
      WHEN baseline_monthly_stddev > 0 AND (monthly_cost - baseline_monthly_mean) / baseline_monthly_stddev >= 5
        THEN 'critical_increase'
      WHEN baseline_monthly_stddev > 0 AND (monthly_cost - baseline_monthly_mean) / baseline_monthly_stddev <= -5
        THEN 'critical_decrease'
      WHEN baseline_monthly_stddev > 0 AND (monthly_cost - baseline_monthly_mean) / baseline_monthly_stddev >= 3
        THEN 'anomalous_increase'
      WHEN baseline_monthly_stddev > 0 AND (monthly_cost - baseline_monthly_mean) / baseline_monthly_stddev <= -3
        THEN 'anomalous_decrease'
      WHEN baseline_monthly_stddev > 0 AND (monthly_cost - baseline_monthly_mean) / baseline_monthly_stddev >= 2
        THEN 'warning'
      ELSE 'normal'
    END
  ) AS monthly,

  NAMED_STRUCT(
    'mean', baseline_monthly_mean,
    'stddev', baseline_monthly_stddev
  ) AS monthly_baseline,

  -- Quarterly anomaly metrics
  NAMED_STRUCT(
    'cost', quarterly_cost,
    'z_score',
    CASE
      WHEN baseline_quarterly_stddev IS NOT NULL AND baseline_quarterly_stddev > 0
      THEN (quarterly_cost - baseline_quarterly_mean) / baseline_quarterly_stddev
      ELSE NULL
    END,
    'percent_change',
    CASE
      WHEN baseline_quarterly_mean IS NOT NULL AND baseline_quarterly_mean > 0
      THEN (quarterly_cost - baseline_quarterly_mean) / baseline_quarterly_mean
      ELSE NULL
    END,
    'status',
    CASE
      WHEN baseline_quarterly_mean IS NULL OR baseline_quarterly_stddev IS NULL
        THEN 'insufficient_history'
      WHEN baseline_quarterly_stddev > 0 AND (quarterly_cost - baseline_quarterly_mean) / baseline_quarterly_stddev >= 5
        THEN 'critical_increase'
      WHEN baseline_quarterly_stddev > 0 AND (quarterly_cost - baseline_quarterly_mean) / baseline_quarterly_stddev <= -5
        THEN 'critical_decrease'
      WHEN baseline_quarterly_stddev > 0 AND (quarterly_cost - baseline_quarterly_mean) / baseline_quarterly_stddev >= 3
        THEN 'anomalous_increase'
      WHEN baseline_quarterly_stddev > 0 AND (quarterly_cost - baseline_quarterly_mean) / baseline_quarterly_stddev <= -3
        THEN 'anomalous_decrease'
      WHEN baseline_quarterly_stddev > 0 AND (quarterly_cost - baseline_quarterly_mean) / baseline_quarterly_stddev >= 2
        THEN 'warning'
      ELSE 'normal'
    END
  ) AS quarterly,

  NAMED_STRUCT(
    'mean', baseline_quarterly_mean,
    'stddev', baseline_quarterly_stddev
  ) AS quarterly_baseline,

  -- Yearly anomaly metrics
  NAMED_STRUCT(
    'cost', yearly_cost,
    'z_score',
    CASE
      WHEN baseline_yearly_stddev IS NOT NULL AND baseline_yearly_stddev > 0
      THEN (yearly_cost - baseline_yearly_mean) / baseline_yearly_stddev
      ELSE NULL
    END,
    'percent_change',
    CASE
      WHEN baseline_yearly_mean IS NOT NULL AND baseline_yearly_mean > 0
      THEN (yearly_cost - baseline_yearly_mean) / baseline_yearly_mean
      ELSE NULL
    END,
    'status',
    CASE
      WHEN baseline_yearly_mean IS NULL OR baseline_yearly_stddev IS NULL
        THEN 'insufficient_history'
      WHEN baseline_yearly_stddev > 0 AND (yearly_cost - baseline_yearly_mean) / baseline_yearly_stddev >= 5
        THEN 'critical_increase'
      WHEN baseline_yearly_stddev > 0 AND (yearly_cost - baseline_yearly_mean) / baseline_yearly_stddev <= -5
        THEN 'critical_decrease'
      WHEN baseline_yearly_stddev > 0 AND (yearly_cost - baseline_yearly_mean) / baseline_yearly_stddev >= 3
        THEN 'anomalous_increase'
      WHEN baseline_yearly_stddev > 0 AND (yearly_cost - baseline_yearly_mean) / baseline_yearly_stddev <= -3
        THEN 'anomalous_decrease'
      WHEN baseline_yearly_stddev > 0 AND (yearly_cost - baseline_yearly_mean) / baseline_yearly_stddev >= 2
        THEN 'warning'
      ELSE 'normal'
    END
  ) AS yearly,

  NAMED_STRUCT(
    'mean', baseline_yearly_mean,
    'stddev', baseline_yearly_stddev
  ) AS yearly_baseline

FROM scored
WHERE date BETWEEN "{date_start}" AND "{date_end}"
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Cost anomaly detection using z-score analysis. Join to dim_cost_hierarchies on date + cost_hierarchy_id for dimension details.",
        row_meaning="Each row represents daily cost anomaly metrics across multiple time horizons for a specific hierarchy.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_COSTS_ROLLING),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TABLE_COST_ANOMALIES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_table_cost_anomalies(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
    )
