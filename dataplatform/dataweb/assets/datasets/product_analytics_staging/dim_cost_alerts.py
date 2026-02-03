"""
Pre-filtered cost alert views for common monitoring scenarios

Provides ready-to-query views of critical, anomalous, and warning-level cost events
across different aggregation levels and time horizons for easy alerting setup.
"""

from dataclasses import replace

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
from dataweb.userpkgs.firmware.hash_utils import (
    GROUPING_COLUMNS_COLUMN,
    HIERARCHY_LABEL_COLUMN,
)
from dataweb.assets.datasets.product_analytics_staging.dim_cost_hierarchies import (
    COST_HIERARCHY_ID_COLUMN,
    REGION_COLUMN,
    TEAM_COLUMN,
    PRODUCT_GROUP_COLUMN,
    DATAPLATFORM_FEATURE_COLUMN,
    SERVICE_COLUMN,
)

# Schema definition
# Order: date, hierarchy IDs, dimension columns, then table-specific columns
COLUMNS = [
    ColumnType.DATE,
    COST_HIERARCHY_ID_COLUMN,
    GROUPING_COLUMNS_COLUMN,
    HIERARCHY_LABEL_COLUMN,
    replace(REGION_COLUMN, primary_key=False, nullable=True),
    replace(TEAM_COLUMN, primary_key=False, nullable=True),
    replace(PRODUCT_GROUP_COLUMN, primary_key=False, nullable=True),
    replace(DATAPLATFORM_FEATURE_COLUMN, primary_key=False, nullable=True),
    replace(SERVICE_COLUMN, nullable=True, primary_key=False),
    # Table-specific columns below
    Column(
        name="alert_type",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Type of alert: critical_service_increase, critical_team_increase, critical_decrease, anomalous_spike, etc."
        ),
    ),
    Column(
        name="time_horizon",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Time horizon: daily, weekly, monthly, quarterly, yearly"
        ),
    ),
    Column(
        name="cost",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Current cost value"),
    ),
    Column(
        name="z_score",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Z-score indicating severity"),
    ),
    Column(
        name="percent_change",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Percent change from baseline"),
    ),
    Column(
        name="baseline_mean",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Historical baseline mean cost"),
    ),
    Column(
        name="status",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Anomaly status"),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
WITH base AS (
  SELECT
    a.date,
    a.cost_hierarchy_id,
    h.grouping_columns,
    h.hierarchy_label,
    h.region,
    h.team,
    h.product_group,
    h.dataplatform_feature,
    h.service,
    a.daily.cost AS daily_cost,
    a.daily.z_score AS daily_z,
    a.daily.percent_change AS daily_pct,
    a.daily.status AS daily_status,
    a.daily_baseline.mean AS daily_baseline,
    a.weekly.cost AS weekly_cost,
    a.weekly.z_score AS weekly_z,
    a.weekly.percent_change AS weekly_pct,
    a.weekly.status AS weekly_status,
    a.weekly_baseline.mean AS weekly_baseline,
    a.monthly.cost AS monthly_cost,
    a.monthly.z_score AS monthly_z,
    a.monthly.percent_change AS monthly_pct,
    a.monthly.status AS monthly_status,
    a.monthly_baseline.mean AS monthly_baseline,
    a.quarterly.status AS quarterly_status,
    a.yearly.status AS yearly_status
  FROM {product_analytics_staging}.fct_table_cost_anomalies a
  JOIN {product_analytics_staging}.dim_cost_hierarchies h
    USING (date, cost_hierarchy_id)
  WHERE a.date BETWEEN "{date_start}" AND "{date_end}"
),

-- Critical service-level increases (most granular, highest priority)
critical_service_increases AS (
  SELECT
    date,
    'critical_service_increase' AS alert_type,
    cost_hierarchy_id,
    grouping_columns,
    hierarchy_label,
    region,
    team,
    product_group,
    dataplatform_feature,
    service,
    'daily' AS time_horizon,
    daily_cost AS cost,
    daily_z AS z_score,
    daily_pct AS percent_change,
    daily_baseline AS baseline_mean,
    daily_status AS status
  FROM base
  WHERE service IS NOT NULL  -- Service-level groupings
    AND daily_status = 'critical_increase'
),

-- Critical team-level increases (cross-region/pipeline view)
critical_team_increases AS (
  SELECT
    date,
    'critical_team_increase' AS alert_type,
    cost_hierarchy_id,
    grouping_columns,
    hierarchy_label,
    region,
    team,
    product_group,
    dataplatform_feature,
    service,
    'daily' AS time_horizon,
    daily_cost AS cost,
    daily_z AS z_score,
    daily_pct AS percent_change,
    daily_baseline AS baseline_mean,
    daily_status AS status
  FROM base
  WHERE team IS NOT NULL AND product_group IS NULL  -- Team-level groupings (not drilled to product)
    AND daily_status = 'critical_increase'
),

-- Critical decreases (may indicate data quality issues)
critical_decreases AS (
  SELECT
    date,
    'critical_decrease' AS alert_type,
    cost_hierarchy_id,
    grouping_columns,
    hierarchy_label,
    region,
    team,
    product_group,
    dataplatform_feature,
    service,
    CASE 
      WHEN daily_status = 'critical_decrease' THEN 'daily'
      WHEN weekly_status = 'critical_decrease' THEN 'weekly'
      WHEN monthly_status = 'critical_decrease' THEN 'monthly'
    END AS time_horizon,
    daily_cost AS cost,
    CASE 
      WHEN daily_status = 'critical_decrease' THEN daily_z
      WHEN weekly_status = 'critical_decrease' THEN weekly_z
      WHEN monthly_status = 'critical_decrease' THEN monthly_z
    END AS z_score,
    CASE 
      WHEN daily_status = 'critical_decrease' THEN daily_pct
      WHEN weekly_status = 'critical_decrease' THEN weekly_pct
      WHEN monthly_status = 'critical_decrease' THEN monthly_pct
    END AS percent_change,
    daily_baseline AS baseline_mean,
    CASE 
      WHEN daily_status = 'critical_decrease' THEN daily_status
      WHEN weekly_status = 'critical_decrease' THEN weekly_status
      WHEN monthly_status = 'critical_decrease' THEN monthly_status
    END AS status
  FROM base
  WHERE daily_status = 'critical_decrease'
     OR weekly_status = 'critical_decrease'
     OR monthly_status = 'critical_decrease'
),

-- Anomalous weekly spikes (sustained issues)
anomalous_weekly_spikes AS (
  SELECT
    date,
    'anomalous_weekly_spike' AS alert_type,
    cost_hierarchy_id,
    grouping_columns,
    hierarchy_label,
    region,
    team,
    product_group,
    dataplatform_feature,
    service,
    'weekly' AS time_horizon,
    weekly_cost AS cost,
    weekly_z AS z_score,
    weekly_pct AS percent_change,
    weekly_baseline AS baseline_mean,
    weekly_status AS status
  FROM base
  WHERE service IS NOT NULL  -- Service-level groupings
    AND weekly_status IN ('anomalous_increase', 'critical_increase')
),

-- Overall daily cost spikes (platform-wide issues)
overall_daily_spikes AS (
  SELECT
    date,
    'overall_daily_spike' AS alert_type,
    cost_hierarchy_id,
    grouping_columns,
    hierarchy_label,
    region,
    team,
    product_group,
    dataplatform_feature,
    service,
    'daily' AS time_horizon,
    daily_cost AS cost,
    daily_z AS z_score,
    daily_pct AS percent_change,
    daily_baseline AS baseline_mean,
    daily_status AS status
  FROM base
  WHERE grouping_columns = 'overall'  -- Overall grouping
    AND daily_status IN ('anomalous_increase', 'critical_increase')
),

-- Multiple horizon anomalies (consistent across timeframes)
multi_horizon_anomalies AS (
  SELECT
    date,
    'multi_horizon_anomaly' AS alert_type,
    cost_hierarchy_id,
    grouping_columns,
    hierarchy_label,
    region,
    team,
    product_group,
    dataplatform_feature,
    service,
    'daily' AS time_horizon,
    daily_cost AS cost,
    daily_z AS z_score,
    daily_pct AS percent_change,
    daily_baseline AS baseline_mean,
    daily_status AS status
  FROM base
  WHERE service IS NOT NULL  -- Service-level groupings
    AND daily_status NOT IN ('normal', 'insufficient_history')
    AND weekly_status NOT IN ('normal', 'insufficient_history')
    AND monthly_status NOT IN ('normal', 'insufficient_history')
)

-- Union all alert types
SELECT * FROM critical_service_increases
UNION ALL
SELECT * FROM critical_team_increases
UNION ALL
SELECT * FROM critical_decreases
UNION ALL
SELECT * FROM anomalous_weekly_spikes
UNION ALL
SELECT * FROM overall_daily_spikes
UNION ALL
SELECT * FROM multi_horizon_anomalies
ORDER BY date, alert_type, hierarchy_label
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc=(
            "Pre-filtered cost alert views for common monitoring scenarios. "
            "Includes critical service/team increases, critical decreases (potential DQ issues), "
            "sustained weekly spikes, platform-wide anomalies, and multi-horizon alerts."
        ),
        row_meaning="Each row represents a specific alert condition triggered for a time series.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TABLE_COST_ANOMALIES),
        AnyUpstream(ProductAnalyticsStaging.DIM_COST_HIERARCHIES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_COST_ALERTS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_cost_alerts(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
    )
