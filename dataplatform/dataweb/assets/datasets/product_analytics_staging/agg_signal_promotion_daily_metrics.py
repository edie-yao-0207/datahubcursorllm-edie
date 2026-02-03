"""
agg_signal_promotion_daily_metrics

Daily aggregated metrics for signal promotions with rolling windows (daily, weekly,
monthly, quarterly, yearly). Shows promotion trial outcomes, depth (signal diversity),
and velocity (throughput) trends over time.

Uses the hierarchy library for consistent hash-based grouping IDs and labels.
"""

from dataclasses import replace
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
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
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    struct_with_comments,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.firmware.signal_promotion_hierarchy import (
    HIERARCHY_ID_SQL,
    GROUPING_COLUMNS_SQL,
    SIGNAL_PROMOTION_HIERARCHY_ID_COLUMN,
    POPULATION_UUID_COLUMN,
)
from dataweb.userpkgs.firmware.hash_utils import GROUPING_COLUMNS_COLUMN
from .fct_signal_promotion_transitions import (
    DATA_SOURCE_COLUMN,
    USER_ID_COLUMN,
)

# Shared metrics struct type for time window aggregations
METRICS_STRUCT_TYPE = struct_with_comments(
    # Velocity metrics (throughput)
    ("total_trials", DataType.LONG, "Count of promotions in window"),
    ("graduations", DataType.LONG, "Promotions that reached BETA (graduated from Alpha)"),
    (
        "beta_rollbacks",
        DataType.LONG,
        "Rollbacks from BETA (production issues, the key problem metric)",
    ),
    (
        "alpha_rollbacks",
        DataType.LONG,
        "Rollbacks from Alpha (expected testing churn)",
    ),
    (
        "in_progress",
        DataType.LONG,
        "In-progress promotions (still being evaluated)",
    ),
    ("graduation_rate", DataType.DOUBLE, "Graduation rate (graduations / total_trials)"),
    (
        "beta_rollback_rate",
        DataType.DOUBLE,
        "Beta rollback rate (production issues, should be minimized)",
    ),
    ("alpha_rollback_rate", DataType.DOUBLE, "Alpha rollback rate (expected testing churn)"),
    ("in_progress_rate", DataType.DOUBLE, "In-progress rate"),
    (
        "avg_time_to_graduation_seconds",
        DataType.LONG,
        "Average seconds from creation to graduation (reaching BETA)",
    ),
    (
        "avg_time_in_current_stage_seconds",
        DataType.LONG,
        "Average seconds in current stage",
    ),
    # Depth metrics (signal diversity) - only accurate in daily struct
    # Rolling window unique counts are NULL because summing daily distinct counts
    # would double-count entities appearing on multiple days
    ("unique_signals", DataType.LONG, "Distinct signals (daily only, NULL in rolling windows)"),
    (
        "unique_populations",
        DataType.LONG,
        "Distinct populations (daily only, NULL in rolling windows)",
    ),
)

CURRENT_STAGE_COLUMN = Column(
    name="current_stage",
    type=DataType.SHORT,
    nullable=True,
    primary_key=True,
    metadata=Metadata(
        comment="Current promotion stage ID. Join to definitions.promotion_stage for name. "
        "Values: 0=INITIAL, 1=FAILED, 2=PRE_ALPHA, 3=ALPHA, 4=BETA, 5=GA"
    ),
)

PROMOTION_OUTCOME_DIM_COLUMN = Column(
    name="promotion_outcome",
    type=DataType.STRING,
    nullable=True,
    primary_key=True,
    metadata=Metadata(
        comment="Promotion outcome: SUCCESS, PRODUCTION_FAILURE, ALPHA_ROLLBACK, IN_PROGRESS"
    ),
)

COLUMNS = [
    ColumnType.DATE,
    SIGNAL_PROMOTION_HIERARCHY_ID_COLUMN,  # Primary key (hash of grouping + values)
    GROUPING_COLUMNS_COLUMN,
    replace(USER_ID_COLUMN, nullable=True, primary_key=False),
    replace(DATA_SOURCE_COLUMN, nullable=True, primary_key=False),
    replace(CURRENT_STAGE_COLUMN, primary_key=False),
    replace(PROMOTION_OUTCOME_DIM_COLUMN, primary_key=False),
    replace(POPULATION_UUID_COLUMN, primary_key=False),
    Column(
        name="daily",
        type=METRICS_STRUCT_TYPE,
        metadata=Metadata(comment="Daily metrics (single day, no rolling window)"),
    ),
    Column(
        name="weekly",
        type=METRICS_STRUCT_TYPE,
        metadata=Metadata(comment="Weekly rolling metrics (7-day trailing window)"),
    ),
    Column(
        name="monthly",
        type=METRICS_STRUCT_TYPE,
        metadata=Metadata(comment="Monthly rolling metrics (30-day trailing window)"),
    ),
    Column(
        name="quarterly",
        type=METRICS_STRUCT_TYPE,
        metadata=Metadata(comment="Quarterly rolling metrics (90-day trailing window)"),
    ),
    Column(
        name="yearly",
        type=METRICS_STRUCT_TYPE,
        metadata=Metadata(comment="Yearly rolling metrics (365-day trailing window)"),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
WITH params AS (
  SELECT
    CAST("{date_start}" AS DATE) AS dmin,
    CAST("{date_end}" AS DATE) AS dmax
),

-- Continuous calendar (extend 364 days before for yearly rolling window)
date_spine AS (
  SELECT explode(sequence(date_sub(dmin, 364), dmax, interval 1 day)) AS date
  FROM params
),

-- Daily counts with GROUPING SETS for all dimension combinations
-- Dimensions: user_id, data_source, current_stage, promotion_outcome, population_uuid
daily_snapshot AS (
  SELECT
    f.date,
    {hierarchy_id_sql} AS hierarchy_id,
    {grouping_columns_sql} AS grouping_columns,
    f.created_by AS user_id,
    f.data_source,
    f.current.stage AS current_stage,
    f.promotion_outcome,
    f.population_uuid,
    -- Velocity metrics
    COUNT(*) AS total_trials,
    SUM(CASE WHEN f.promotion_outcome = 'SUCCESS' THEN 1 ELSE 0 END) AS graduation_count,
    SUM(CASE WHEN f.promotion_outcome = 'PRODUCTION_FAILURE' THEN 1 ELSE 0 END) AS beta_rollback_count,
    SUM(CASE WHEN f.promotion_outcome = 'ALPHA_ROLLBACK' THEN 1 ELSE 0 END) AS alpha_rollback_count,
    SUM(CASE WHEN f.promotion_outcome = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress_count,
    CAST(AVG(CASE WHEN f.promotion_outcome = 'SUCCESS' THEN f.time_to_success_seconds END) AS LONG) AS avg_time_to_graduation_seconds,
    CAST(AVG(f.time_in_current_stage_seconds) AS LONG) AS avg_time_in_current_stage_seconds,
    -- Depth metrics
    COUNT(DISTINCT f.signal_uuid) AS unique_signals,
    COUNT(DISTINCT f.population_uuid) AS unique_populations
  FROM {product_analytics_staging}.fct_signal_promotion_latest_state f
  JOIN params
  WHERE f.date BETWEEN date_sub(dmin, 364) AND dmax
  GROUP BY GROUPING SETS (
    -- Overall (no dimensions)
    (f.date),
    -- Single dimensions
    (f.date, f.created_by),
    (f.date, f.data_source),
    (f.date, f.current.stage),
    (f.date, f.promotion_outcome),
    (f.date, f.population_uuid),
    -- Two dimensions
    (f.date, f.created_by, f.data_source),
    (f.date, f.created_by, f.current.stage),
    (f.date, f.created_by, f.promotion_outcome),
    (f.date, f.created_by, f.population_uuid),
    (f.date, f.data_source, f.current.stage),
    (f.date, f.data_source, f.promotion_outcome),
    (f.date, f.data_source, f.population_uuid),
    (f.date, f.current.stage, f.promotion_outcome),
    (f.date, f.current.stage, f.population_uuid),
    (f.date, f.promotion_outcome, f.population_uuid),
    -- Three dimensions
    (f.date, f.created_by, f.data_source, f.current.stage),
    (f.date, f.created_by, f.data_source, f.promotion_outcome),
    (f.date, f.created_by, f.data_source, f.population_uuid),
    (f.date, f.created_by, f.current.stage, f.promotion_outcome),
    (f.date, f.created_by, f.current.stage, f.population_uuid),
    (f.date, f.created_by, f.promotion_outcome, f.population_uuid),
    (f.date, f.data_source, f.current.stage, f.promotion_outcome),
    (f.date, f.data_source, f.current.stage, f.population_uuid),
    (f.date, f.data_source, f.promotion_outcome, f.population_uuid),
    (f.date, f.current.stage, f.promotion_outcome, f.population_uuid),
    -- Four dimensions
    (f.date, f.created_by, f.data_source, f.current.stage, f.promotion_outcome),
    (f.date, f.created_by, f.data_source, f.current.stage, f.population_uuid),
    (f.date, f.created_by, f.data_source, f.promotion_outcome, f.population_uuid),
    (f.date, f.created_by, f.current.stage, f.promotion_outcome, f.population_uuid),
    (f.date, f.data_source, f.current.stage, f.promotion_outcome, f.population_uuid),
    -- All dimensions
    (f.date, f.created_by, f.data_source, f.current.stage, f.promotion_outcome, f.population_uuid)
  )
),

-- Get distinct dimension combinations that actually exist in the data
dimension_combos AS (
  SELECT DISTINCT
    hierarchy_id,
    grouping_columns,
    user_id,
    data_source,
    current_stage,
    promotion_outcome,
    population_uuid
  FROM daily_snapshot
),

-- Zero-fill dates only for dimension combinations that have data
daily_filled AS (
  SELECT
    ds.date,
    dc.hierarchy_id,
    dc.grouping_columns,
    dc.user_id,
    dc.data_source,
    dc.current_stage,
    dc.promotion_outcome,
    dc.population_uuid,
    COALESCE(d.total_trials, 0) AS total_trials,
    COALESCE(d.graduation_count, 0) AS graduation_count,
    COALESCE(d.beta_rollback_count, 0) AS beta_rollback_count,
    COALESCE(d.alpha_rollback_count, 0) AS alpha_rollback_count,
    COALESCE(d.in_progress_count, 0) AS in_progress_count,
    COALESCE(d.avg_time_to_graduation_seconds, 0) AS avg_time_to_graduation_seconds,
    COALESCE(d.avg_time_in_current_stage_seconds, 0) AS avg_time_in_current_stage_seconds,
    COALESCE(d.unique_signals, 0) AS unique_signals,
    COALESCE(d.unique_populations, 0) AS unique_populations
  FROM date_spine ds
  CROSS JOIN dimension_combos dc
  LEFT JOIN daily_snapshot d ON d.date = ds.date AND d.hierarchy_id = dc.hierarchy_id
),

-- Rolling windows: daily (0), weekly (6), monthly (29), quarterly (89), yearly (364) preceding days
-- Helper function to build metrics struct with rates
-- Computed inline for each time window
rolling AS (
  SELECT
    date,
    hierarchy_id,
    grouping_columns,
    user_id,
    data_source,
    current_stage,
    promotion_outcome,
    population_uuid,
    -- Daily struct
    STRUCT(
      total_trials AS total_trials,
      graduation_count AS graduations,
      beta_rollback_count AS beta_rollbacks,
      alpha_rollback_count AS alpha_rollbacks,
      in_progress_count AS in_progress,
      CAST(graduation_count AS DOUBLE) / NULLIF(total_trials, 0) AS graduation_rate,
      CAST(beta_rollback_count AS DOUBLE) / NULLIF(total_trials, 0) AS beta_rollback_rate,
      CAST(alpha_rollback_count AS DOUBLE) / NULLIF(total_trials, 0) AS alpha_rollback_rate,
      CAST(in_progress_count AS DOUBLE) / NULLIF(total_trials, 0) AS in_progress_rate,
      avg_time_to_graduation_seconds,
      avg_time_in_current_stage_seconds,
      unique_signals,
      unique_populations
    ) AS daily,
    -- Weekly struct (7-day rolling)
    STRUCT(
      SUM(total_trials) OVER w7 AS total_trials,
      SUM(graduation_count) OVER w7 AS graduations,
      SUM(beta_rollback_count) OVER w7 AS beta_rollbacks,
      SUM(alpha_rollback_count) OVER w7 AS alpha_rollbacks,
      SUM(in_progress_count) OVER w7 AS in_progress,
      CAST(SUM(graduation_count) OVER w7 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w7, 0) AS graduation_rate,
      CAST(SUM(beta_rollback_count) OVER w7 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w7, 0) AS beta_rollback_rate,
      CAST(SUM(alpha_rollback_count) OVER w7 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w7, 0) AS alpha_rollback_rate,
      CAST(SUM(in_progress_count) OVER w7 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w7, 0) AS in_progress_rate,
      CAST(AVG(avg_time_to_graduation_seconds) OVER w7 AS LONG) AS avg_time_to_graduation_seconds,
      CAST(AVG(avg_time_in_current_stage_seconds) OVER w7 AS LONG) AS avg_time_in_current_stage_seconds,
      -- NULL: accurate window-level distinct counts cannot be computed from daily aggregates
      CAST(NULL AS LONG) AS unique_signals,
      CAST(NULL AS LONG) AS unique_populations
    ) AS weekly,
    -- Monthly struct (30-day rolling)
    STRUCT(
      SUM(total_trials) OVER w30 AS total_trials,
      SUM(graduation_count) OVER w30 AS graduations,
      SUM(beta_rollback_count) OVER w30 AS beta_rollbacks,
      SUM(alpha_rollback_count) OVER w30 AS alpha_rollbacks,
      SUM(in_progress_count) OVER w30 AS in_progress,
      CAST(SUM(graduation_count) OVER w30 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w30, 0) AS graduation_rate,
      CAST(SUM(beta_rollback_count) OVER w30 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w30, 0) AS beta_rollback_rate,
      CAST(SUM(alpha_rollback_count) OVER w30 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w30, 0) AS alpha_rollback_rate,
      CAST(SUM(in_progress_count) OVER w30 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w30, 0) AS in_progress_rate,
      CAST(AVG(avg_time_to_graduation_seconds) OVER w30 AS LONG) AS avg_time_to_graduation_seconds,
      CAST(AVG(avg_time_in_current_stage_seconds) OVER w30 AS LONG) AS avg_time_in_current_stage_seconds,
      -- NULL: accurate window-level distinct counts cannot be computed from daily aggregates
      CAST(NULL AS LONG) AS unique_signals,
      CAST(NULL AS LONG) AS unique_populations
    ) AS monthly,
    -- Quarterly struct (90-day rolling)
    STRUCT(
      SUM(total_trials) OVER w90 AS total_trials,
      SUM(graduation_count) OVER w90 AS graduations,
      SUM(beta_rollback_count) OVER w90 AS beta_rollbacks,
      SUM(alpha_rollback_count) OVER w90 AS alpha_rollbacks,
      SUM(in_progress_count) OVER w90 AS in_progress,
      CAST(SUM(graduation_count) OVER w90 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w90, 0) AS graduation_rate,
      CAST(SUM(beta_rollback_count) OVER w90 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w90, 0) AS beta_rollback_rate,
      CAST(SUM(alpha_rollback_count) OVER w90 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w90, 0) AS alpha_rollback_rate,
      CAST(SUM(in_progress_count) OVER w90 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w90, 0) AS in_progress_rate,
      CAST(AVG(avg_time_to_graduation_seconds) OVER w90 AS LONG) AS avg_time_to_graduation_seconds,
      CAST(AVG(avg_time_in_current_stage_seconds) OVER w90 AS LONG) AS avg_time_in_current_stage_seconds,
      -- NULL: accurate window-level distinct counts cannot be computed from daily aggregates
      CAST(NULL AS LONG) AS unique_signals,
      CAST(NULL AS LONG) AS unique_populations
    ) AS quarterly,
    -- Yearly struct (365-day rolling)
    STRUCT(
      SUM(total_trials) OVER w365 AS total_trials,
      SUM(graduation_count) OVER w365 AS graduations,
      SUM(beta_rollback_count) OVER w365 AS beta_rollbacks,
      SUM(alpha_rollback_count) OVER w365 AS alpha_rollbacks,
      SUM(in_progress_count) OVER w365 AS in_progress,
      CAST(SUM(graduation_count) OVER w365 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w365, 0) AS graduation_rate,
      CAST(SUM(beta_rollback_count) OVER w365 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w365, 0) AS beta_rollback_rate,
      CAST(SUM(alpha_rollback_count) OVER w365 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w365, 0) AS alpha_rollback_rate,
      CAST(SUM(in_progress_count) OVER w365 AS DOUBLE) / NULLIF(SUM(total_trials) OVER w365, 0) AS in_progress_rate,
      CAST(AVG(avg_time_to_graduation_seconds) OVER w365 AS LONG) AS avg_time_to_graduation_seconds,
      CAST(AVG(avg_time_in_current_stage_seconds) OVER w365 AS LONG) AS avg_time_in_current_stage_seconds,
      -- NULL: accurate window-level distinct counts cannot be computed from daily aggregates
      CAST(NULL AS LONG) AS unique_signals,
      CAST(NULL AS LONG) AS unique_populations
    ) AS yearly
  FROM daily_filled
  WINDOW
    w7 AS (PARTITION BY hierarchy_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    w30 AS (PARTITION BY hierarchy_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
    w90 AS (PARTITION BY hierarchy_id ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
    w365 AS (PARTITION BY hierarchy_id ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW)
)

SELECT
  CAST(date AS STRING) AS date,
  hierarchy_id,
  grouping_columns,
  user_id,
  data_source,
  current_stage,
  promotion_outcome,
  population_uuid,
  daily,
  weekly,
  monthly,
  quarterly,
  yearly
FROM rolling
WHERE date BETWEEN "{date_start}" AND "{date_end}"
ORDER BY date, hierarchy_id, user_id NULLS FIRST, data_source NULLS FIRST, current_stage NULLS FIRST, promotion_outcome NULLS FIRST, population_uuid NULLS FIRST
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily signal promotion metrics with multiple rolling windows (daily, weekly, monthly, quarterly, yearly) "
        "across multiple dimensions (user_id, data_source, current_stage, promotion_outcome, population). "
        "Uses GROUPING SETS to efficiently compute metrics at all dimension combinations in a single query. "
        "Filter by grouping_columns (e.g., overall, user, data_source, user.data_source, stage.outcome, population). "
        "Each time window struct includes velocity metrics (total_trials, graduations, rates, time_to_graduation) "
        "and depth metrics (unique_signals, unique_populations). "
        "Join to signalpromotiondb.populations on population_uuid for vehicle details (make, model, year). "
        "Uses hierarchy library for consistent hash-based hierarchy_id. "
        "Key metrics: graduation_rate (reached BETA), beta_rollback_rate (production issues to minimize), alpha_rollback_rate (expected churn).",
        row_meaning="Each row represents promotion metrics at multiple time windows for a specific date and dimension combination. "
        "Use grouping_columns = overall for team-wide metrics.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_SIGNAL_PROMOTION_LATEST_STATE),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_SIGNAL_PROMOTION_DAILY_METRICS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_signal_promotion_daily_metrics(context: AssetExecutionContext) -> str:
    """
    Multi-dimensional signal promotion metrics with multiple rolling windows.

    Uses GROUPING SETS to efficiently compute metrics across all dimension combinations.
    Uses hierarchy library for consistent hash-based hierarchy_id and grouping_columns.

    **Dimensions** (filter using grouping_columns):
    - user: User who created the promotion
    - data_source: Signal source type (J1939, UDS, J1979, etc.)
    - stage: Current promotion stage (INITIAL, FAILED, PRE_ALPHA, ALPHA, BETA, GA)
    - outcome: Outcome classification (SUCCESS, PRODUCTION_FAILURE, ALPHA_ROLLBACK, IN_PROGRESS)
    - population: Population UUID (includes make/model/year/engine details)

    **Grouping Columns** (dot-separated active dimensions):
    - 'overall': No dimensions, team-wide aggregate
    - 'user': By user only
    - 'data_source': By data source only
    - 'population': By population (with make/model/year details)
    - 'user.data_source': By user and data source
    - 'stage.outcome': By stage and outcome
    - etc. (all 32 combinations)

    **Population Details**:
    - Join to signalpromotiondb.populations on population_uuid for vehicle details

    **Time Windows** (each as a nested struct):
    - daily: Current day metrics (no rolling)
    - weekly: Rolling 7-day window
    - monthly: Rolling 30-day window
    - quarterly: Rolling 90-day window
    - yearly: Rolling 365-day window

    **Velocity Metrics in Each Window Struct**:
    - total_trials: Promotions with latest update in window
    - graduations: Promotions that reached BETA (graduated from Alpha)
    - beta_rollbacks: Rollbacks from BETA (production issues - key problem metric)
    - alpha_rollbacks: Rollbacks from Alpha (expected testing churn)
    - in_progress: Promotions still being evaluated
    - Rates: graduation_rate, beta_rollback_rate, alpha_rollback_rate, in_progress_rate
    - avg_time_to_graduation_seconds, avg_time_in_current_stage_seconds

    **Depth Metrics in Each Window Struct**:
    - unique_signals: Count of distinct signals promoted
    - unique_populations: Count of distinct populations with promotions

    **Example Queries**:
    ```sql
    -- Overall monthly graduation rate and signal diversity
    SELECT date, monthly.graduation_rate, monthly.unique_signals
    FROM ... WHERE grouping_columns = 'overall'

    -- Yearly trends by data source
    SELECT date, data_source, yearly.total_trials, yearly.graduation_rate
    FROM ... WHERE grouping_columns = 'data_source'

    -- Metrics by population (join to populations for vehicle details)
    SELECT m.date, p.make, p.model, p.year,
           m.monthly.total_trials, m.monthly.graduation_rate
    FROM ... m
    LEFT JOIN signalpromotiondb.populations p ON m.population_uuid = p.population_uuid
    WHERE m.grouping_columns = 'population'

    -- Beta rollback rate (production issues) over time
    SELECT date, monthly.beta_rollback_rate, monthly.alpha_rollback_rate
    FROM ... WHERE grouping_columns = 'overall'
    ```

    Note: graduation_rate + beta_rollback_rate + alpha_rollback_rate + in_progress_rate = 100%
    (outcomes are mutually exclusive).
    """
    return format_date_partition_query(
        QUERY,
        context,
        hierarchy_id_sql=HIERARCHY_ID_SQL,
        grouping_columns_sql=GROUPING_COLUMNS_SQL,
    )
