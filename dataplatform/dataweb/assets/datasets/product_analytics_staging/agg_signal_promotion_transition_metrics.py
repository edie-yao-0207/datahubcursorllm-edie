"""
agg_signal_promotion_transition_metrics

Daily aggregated metrics for signal promotion transitions with rolling windows.
Groups by (from_stage, from_status) → (to_stage, to_status) to show pipeline flow.

Use this table to:
- Identify bottlenecks (where transitions stall)
- Calculate success/failure rates at each stage
- Track approval/rejection patterns
- Measure rollback rates from production
"""

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
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    struct_with_comments,
    ColumnType,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.firmware.hash_utils import (
    build_grouping_id_sql,
    build_grouping_columns_sql,
    HIERARCHY_ID_COLUMN,
    GROUPING_COLUMNS_COLUMN,
)

# Transition dimensions - the core of this table
FROM_STAGE_COLUMN = Column(
    name="from_stage",
    type=DataType.SHORT,
    nullable=True,
    primary_key=True,
    metadata=Metadata(
        comment="Previous stage ID before transition. NULL for first transition (creation). "
        "Values: 0=INITIAL, 1=FAILED, 2=PRE_ALPHA, 3=ALPHA, 4=BETA, 5=GA. "
        "Join to definitions.promotion_stage for names."
    ),
)

FROM_STATUS_COLUMN = Column(
    name="from_status",
    type=DataType.SHORT,
    nullable=True,
    primary_key=True,
    metadata=Metadata(
        comment="Previous status ID before transition. NULL for first transition. "
        "Values: 0=UNDEFINED, 1=LIVE, 2=PENDING_APPROVAL, 3=PENDING_ROLLBACK. "
        "Join to definitions.promotion_status for names."
    ),
)

TO_STAGE_COLUMN = Column(
    name="to_stage",
    type=DataType.SHORT,
    nullable=True,
    primary_key=True,
    metadata=Metadata(
        comment="New stage ID after transition. "
        "Values: 0=INITIAL, 1=FAILED, 2=PRE_ALPHA, 3=ALPHA, 4=BETA, 5=GA. "
        "Join to definitions.promotion_stage for names."
    ),
)

TO_STATUS_COLUMN = Column(
    name="to_status",
    type=DataType.SHORT,
    nullable=True,
    primary_key=True,
    metadata=Metadata(
        comment="New status ID after transition. "
        "Values: 0=UNDEFINED, 1=LIVE, 2=PENDING_APPROVAL, 3=PENDING_ROLLBACK. "
        "Join to definitions.promotion_status for names."
    ),
)

# Optional grouping dimensions
DATA_SOURCE_COLUMN = Column(
    name="data_source",
    type=DataType.INTEGER,
    nullable=True,
    primary_key=True,
    metadata=Metadata(
        comment="Data source ID. Join to definitions.promotion_data_source_to_name for name."
    ),
)

USER_ID_COLUMN = Column(
    name="user_id",
    type=DataType.LONG,
    nullable=True,
    primary_key=True,
    metadata=Metadata(
        comment="User ID who performed this transition. "
        "Join to datamodel_platform.dim_users for user details."
    ),
)

# Metrics struct for time window aggregations
# Note: unique_* fields are only accurate in daily struct. Rolling window unique counts
# are NULL because summing daily distinct counts would double-count entities appearing
# on multiple days.
METRICS_STRUCT_TYPE = struct_with_comments(
    ("transition_count", DataType.LONG, "Number of transitions in window"),
    ("unique_promotions", DataType.LONG, "Distinct promotions (daily only, NULL in rolling windows)"),
    ("unique_signals", DataType.LONG, "Distinct signals (daily only, NULL in rolling windows)"),
    ("unique_populations", DataType.LONG, "Distinct populations (daily only, NULL in rolling windows)"),
)

COLUMNS = [
    ColumnType.DATE,
    HIERARCHY_ID_COLUMN,
    GROUPING_COLUMNS_COLUMN,
    FROM_STAGE_COLUMN,
    FROM_STATUS_COLUMN,
    TO_STAGE_COLUMN,
    TO_STATUS_COLUMN,
    DATA_SOURCE_COLUMN,
    USER_ID_COLUMN,
    Column(
        name="daily",
        type=METRICS_STRUCT_TYPE,
        metadata=Metadata(comment="Daily metrics (single day)"),
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

# Hierarchy dimensions for GROUPING SETS
# Order: transition info first (most important), then optional dimensions
HIERARCHY_COLUMNS = [
    "t.previous.stage",
    "t.previous.status",
    "t.current.stage",
    "t.current.status",
    "t.data_source",
    "t.user_id",
]

HIERARCHY_ALIASES = [
    "from_stage",
    "from_status",
    "to_stage",
    "to_status",
    "data_source",
    "user",
]

# Generate SQL for hierarchy_id and grouping_columns using utilities
HIERARCHY_ID_SQL = build_grouping_id_sql(HIERARCHY_COLUMNS)
GROUPING_COLUMNS_SQL = build_grouping_columns_sql(HIERARCHY_COLUMNS, HIERARCHY_ALIASES)

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

-- Daily transition counts with GROUPING SETS
daily_snapshot AS (
  SELECT
    t.created_at_date AS date,
    {hierarchy_id_sql} AS hierarchy_id,
    {grouping_columns_sql} AS grouping_columns,
    t.previous.stage AS from_stage,
    t.previous.status AS from_status,
    t.current.stage AS to_stage,
    t.current.status AS to_status,
    t.data_source,
    t.user_id,
    COUNT(*) AS transition_count,
    COUNT(DISTINCT t.promotion_uuid) AS unique_promotions,
    COUNT(DISTINCT t.signal_uuid) AS unique_signals,
    COUNT(DISTINCT t.population_uuid) AS unique_populations
  FROM {product_analytics_staging}.fct_signal_promotion_transitions t
  JOIN params
  WHERE t.created_at_date BETWEEN date_sub(dmin, 364) AND dmax
  GROUP BY GROUPING SETS (
    -- Overall (no dimensions) - total transitions
    (t.created_at_date),
    
    -- Transition only (the core use case)
    (t.created_at_date, t.previous.stage, t.previous.status, t.current.stage, t.current.status),
    
    -- Transition + data_source
    (t.created_at_date, t.previous.stage, t.previous.status, t.current.stage, t.current.status, t.data_source),
    
    -- Transition + user
    (t.created_at_date, t.previous.stage, t.previous.status, t.current.stage, t.current.status, t.user_id),
    
    -- Transition + data_source + user
    (t.created_at_date, t.previous.stage, t.previous.status, t.current.stage, t.current.status, t.data_source, t.user_id),
    
    -- Stage-only transitions (ignoring status for coarser view)
    (t.created_at_date, t.previous.stage, t.current.stage),
    
    -- By data_source only
    (t.created_at_date, t.data_source),
    
    -- By user only
    (t.created_at_date, t.user_id),
    
    -- By data_source and user
    (t.created_at_date, t.data_source, t.user_id)
  )
),

-- Get distinct dimension combinations that exist in data
dimension_combos AS (
  SELECT DISTINCT
    hierarchy_id,
    grouping_columns,
    from_stage,
    from_status,
    to_stage,
    to_status,
    data_source,
    user_id
  FROM daily_snapshot
),

-- Zero-fill dates for dimension combinations
daily_filled AS (
  SELECT
    ds.date,
    dc.hierarchy_id,
    dc.grouping_columns,
    dc.from_stage,
    dc.from_status,
    dc.to_stage,
    dc.to_status,
    dc.data_source,
    dc.user_id,
    COALESCE(d.transition_count, 0) AS transition_count,
    COALESCE(d.unique_promotions, 0) AS unique_promotions,
    COALESCE(d.unique_signals, 0) AS unique_signals,
    COALESCE(d.unique_populations, 0) AS unique_populations
  FROM date_spine ds
  CROSS JOIN dimension_combos dc
  LEFT JOIN daily_snapshot d ON d.date = ds.date AND d.hierarchy_id = dc.hierarchy_id
),

-- Rolling windows with structs built inline
rolling AS (
  SELECT
    date,
    hierarchy_id,
    grouping_columns,
    from_stage,
    from_status,
    to_stage,
    to_status,
    data_source,
    user_id,
    -- Daily struct
    STRUCT(
      transition_count,
      unique_promotions,
      unique_signals,
      unique_populations
    ) AS daily,
    -- Weekly struct (7-day)
    STRUCT(
      SUM(transition_count) OVER w7 AS transition_count,
      -- NULL: accurate window-level distinct counts cannot be computed from daily aggregates
      CAST(NULL AS LONG) AS unique_promotions,
      CAST(NULL AS LONG) AS unique_signals,
      CAST(NULL AS LONG) AS unique_populations
    ) AS weekly,
    -- Monthly struct (30-day)
    STRUCT(
      SUM(transition_count) OVER w30 AS transition_count,
      -- NULL: accurate window-level distinct counts cannot be computed from daily aggregates
      CAST(NULL AS LONG) AS unique_promotions,
      CAST(NULL AS LONG) AS unique_signals,
      CAST(NULL AS LONG) AS unique_populations
    ) AS monthly,
    -- Quarterly struct (90-day)
    STRUCT(
      SUM(transition_count) OVER w90 AS transition_count,
      -- NULL: accurate window-level distinct counts cannot be computed from daily aggregates
      CAST(NULL AS LONG) AS unique_promotions,
      CAST(NULL AS LONG) AS unique_signals,
      CAST(NULL AS LONG) AS unique_populations
    ) AS quarterly,
    -- Yearly struct (365-day)
    STRUCT(
      SUM(transition_count) OVER w365 AS transition_count,
      -- NULL: accurate window-level distinct counts cannot be computed from daily aggregates
      CAST(NULL AS LONG) AS unique_promotions,
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
  from_stage,
  from_status,
  to_stage,
  to_status,
  data_source,
  user_id,
  daily,
  weekly,
  monthly,
  quarterly,
  yearly
FROM rolling
WHERE date BETWEEN "{date_start}" AND "{date_end}"
ORDER BY date, hierarchy_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily signal promotion transition metrics with rolling windows. "
        "Groups by (from_stage, from_status) → (to_stage, to_status) to show pipeline flow. "
        "Use grouping_columns to filter: overall (total), from_stage.from_status.to_stage.to_status (full transitions), "
        "from_stage.to_stage (stage-only), or combined with data_source/user. "
        "Key transitions: Alpha(3)+Live(1) → Beta(4)+Live(1) = SUCCESS. "
        "Beta(4)+Live(1) → Failed(1)+* = PRODUCTION_FAILURE. "
        "Calculate rates by dividing transition counts (e.g., Alpha→Beta / Alpha→anything).",
        row_meaning="Each row represents transition metrics at multiple time windows for a specific date, "
        "transition type, and optional dimension combination.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_SIGNAL_PROMOTION_TRANSITIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_SIGNAL_PROMOTION_TRANSITION_METRICS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_signal_promotion_transition_metrics(context: AssetExecutionContext) -> str:
    """
    Signal promotion transition flow metrics with rolling windows.

    This table shows the FLOW through the promotion pipeline by grouping on
    (from_stage, from_status) → (to_stage, to_status) transitions.

    **Key Transition Types:**

    | from_stage | from_status | to_stage | to_status | Meaning |
    |------------|-------------|----------|-----------|---------|
    | 3 (ALPHA) | 1 (LIVE) | 4 (BETA) | 1 (LIVE) | SUCCESS - promoted to production |
    | 4 (BETA) | 1 (LIVE) | 1 (FAILED) | * | PRODUCTION_FAILURE - rollback |
    | 3 (ALPHA) | 1 (LIVE) | 1 (FAILED) | * | ALPHA_ROLLBACK - testing failure |
    | NULL | NULL | 0 (INITIAL) | * | New promotion created |
    | 3 (ALPHA) | 1 (LIVE) | 3 (ALPHA) | 2 (PENDING) | Submitted for approval |

    **Grouping Columns** (filter by these):
    - 'overall' - total transitions across all types
    - 'from_stage.from_status.to_stage.to_status' - full transition detail
    - 'from_stage.to_stage' - stage-only (coarser view)
    - 'from_stage.from_status.to_stage.to_status.data_source' - by data source
    - 'from_stage.from_status.to_stage.to_status.user' - by user

    **Example Queries:**

    ```sql
    -- Alpha success rate (Alpha → Beta / Alpha → anything)
    WITH alpha_out AS (
      SELECT SUM(monthly.transition_count) AS total
      FROM ... WHERE from_stage = 3 AND grouping_columns = 'from_stage.to_stage'
    ),
    alpha_to_beta AS (
      SELECT SUM(monthly.transition_count) AS success
      FROM ... WHERE from_stage = 3 AND to_stage = 4
        AND grouping_columns = 'from_stage.to_stage'
    )
    SELECT success / total AS alpha_success_rate FROM ...

    -- Production rollback rate
    SELECT 
      date,
      monthly.transition_count AS rollbacks
    FROM ...
    WHERE from_stage = 4 AND to_stage = 1  -- Beta → Failed
      AND grouping_columns = 'from_stage.to_stage'

    -- Transition flow by data source
    SELECT data_source, from_stage, to_stage, monthly.transition_count
    FROM ...
    WHERE grouping_columns = 'from_stage.to_stage.data_source'
    ```
    """
    return format_date_partition_query(
        QUERY,
        context,
        hierarchy_id_sql=HIERARCHY_ID_SQL,
        grouping_columns_sql=GROUPING_COLUMNS_SQL,
    )

