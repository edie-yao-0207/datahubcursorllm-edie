"""
fct_signal_promotion_latest_state

Captures the latest state of each signal promotion with outcome classification.
Partitionless table that shows current status of all promotions.
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
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.query import (
    format_query,
)
from .fct_signal_promotion_transitions import (
    PROMOTION_UUID_COLUMN,
    POPULATION_UUID_COLUMN,
    DATA_SOURCE_COLUMN,
    CREATED_BY_COLUMN,
    CURRENT_STATE_COLUMN,
    PREVIOUS_STATE_COLUMN,
    COMMENT_COLUMN,
    PARSED_COMMENT_COLUMN,
    PROMOTION_OUTCOME_COLUMN,
)

COLUMNS = [
    PROMOTION_UUID_COLUMN,
    ColumnType.SIGNAL_UUID,
    POPULATION_UUID_COLUMN,
    DATA_SOURCE_COLUMN,
    ColumnType.DATE,
    CURRENT_STATE_COLUMN,
    PREVIOUS_STATE_COLUMN,
    Column(
        name="record_count",
        type=DataType.LONG,
        metadata=Metadata(comment="Total number of transitions for this promotion"),
    ),
    CREATED_BY_COLUMN,
    COMMENT_COLUMN,
    PARSED_COMMENT_COLUMN,
    PROMOTION_OUTCOME_COLUMN,
    Column(
        name="time_to_current_state_seconds",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Seconds from promotion creation to current state"),
    ),
    Column(
        name="time_in_current_stage_seconds",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Seconds spent in current stage (from most recent stage transition)"
        ),
    ),
    Column(
        name="time_to_success_seconds",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Seconds from creation to reaching success (BETA+LIVE). NULL if not successful"
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
-- Get latest transition per promotion and count total transitions
WITH latest_transitions AS (
  SELECT
    promotion_uuid,
    signal_uuid,
    population_uuid,
    data_source,
    CAST(created_at_date AS STRING) AS date,
    current,
    previous,
    created_by,
    comment,
    parsed_comment,
    promotion_outcome,
    ROW_NUMBER() OVER (
      PARTITION BY promotion_uuid
      ORDER BY created_at_ms DESC
    ) AS rn
  FROM {product_analytics_staging}.fct_signal_promotion_transitions
),

latest_state AS (
  SELECT
    promotion_uuid,
    signal_uuid,
    population_uuid,
    data_source,
    date,
    current,
    previous,
    created_by,
    comment,
    parsed_comment,
    promotion_outcome
  FROM latest_transitions
  WHERE rn = 1
),

promotion_record_counts AS (
  SELECT
    promotion_uuid,
    COUNT(*) AS record_count
  FROM {product_analytics_staging}.fct_signal_promotion_transitions
  GROUP BY promotion_uuid
),

-- Get timeline metrics: first creation, success time, and stage entry time
promotion_timeline AS (
  SELECT 
    t.promotion_uuid,
    MIN(t.created_at_ms) AS first_created_at_ms,
    MAX(CASE WHEN t.promotion_outcome = 'SUCCESS' THEN t.created_at_ms END) AS success_at_ms,
    MAX(CASE WHEN t.current.stage = l.current.stage THEN t.created_at_ms END) AS entered_current_stage_ms
  FROM {product_analytics_staging}.fct_signal_promotion_transitions t
  JOIN latest_state l ON t.promotion_uuid = l.promotion_uuid
  GROUP BY t.promotion_uuid
)

SELECT
  l.promotion_uuid,
  l.signal_uuid,
  l.population_uuid,
  l.data_source,
  l.date,
  l.current,
  l.previous,
  c.record_count,
  l.created_by,
  l.comment,
  l.parsed_comment,
  l.promotion_outcome,
  
  -- Time metrics in seconds
  CAST((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) * 1000 - t.first_created_at_ms) / 1000.0 AS LONG) AS time_to_current_state_seconds,
  CAST((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) * 1000 - t.entered_current_stage_ms) / 1000.0 AS LONG) AS time_in_current_stage_seconds,
  CASE 
    WHEN l.promotion_outcome = 'SUCCESS' THEN CAST((t.success_at_ms - t.first_created_at_ms) / 1000.0 AS LONG)
    ELSE NULL 
  END AS time_to_success_seconds

FROM latest_state l
LEFT JOIN promotion_record_counts c USING (promotion_uuid)
LEFT JOIN promotion_timeline t USING (promotion_uuid)
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Latest state of each signal promotion with outcome classification. "
        "Derived from fct_signal_promotion_transitions by selecting the most recent transition per promotion. "
        "Tracks current stage, status, latest activity, and promotion_outcome enum (SUCCESS, PRODUCTION_FAILURE, ALPHA_ROLLBACK, IN_PROGRESS). "
        "Stores IDs only; join to definitions.promotion_stage/promotion_status/activity_type for human-readable names. "
        "Includes record_count showing total number of transitions. "
        "SUCCESS = BETA+LIVE. PRODUCTION_FAILURE = rollback from BETA/GA. ALPHA_ROLLBACK = rollback from Alpha testing. IN_PROGRESS = all other states.",
        row_meaning="Each row represents the latest state of one signal promotion with mutually exclusive outcome classification.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_SIGNAL_PROMOTION_TRANSITIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_SIGNAL_PROMOTION_LATEST_STATE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_signal_promotion_latest_state(context: AssetExecutionContext) -> str:
    """
    Latest state snapshot of all signal promotions.

    This partitionless fact table provides the current state of each promotion:

    1. **Latest State**: Most recent date, stage, status, and activity for each promotion_uuid
       - Stores IDs only; join to definitions.promotion_stage/promotion_status/activity_type for names
       - stage: 0=INITIAL, 1=FAILED, 2=PRE_ALPHA, 3=ALPHA, 4=BETA, 5=GA
       - status: 0=UNDEFINED, 1=LIVE, 2=PENDING_APPROVAL, 3=PENDING_ROLLBACK
       - latest_activity: Most recent activity from activity_history (CREATED, APPROVED, REJECTED, etc.)
       - record_count: Number of records in source table for this promotion

    2. **Outcome Classification** (mutually exclusive promotion_outcome enum):
       - **SUCCESS**: stage=4 (BETA) + status=1 (LIVE) - successful production promotion
       - **PRODUCTION_FAILURE**: rollback from BETA(4) or GA(5) to FAILED(1) - genuine regression
       - **ALPHA_ROLLBACK**: rollback from ALPHA(3) to FAILED(1) - expected testing churn
       - **IN_PROGRESS**: All other states (early-stage, transitions, other failures)

    3. **Time Metrics**:
       - time_to_current_state_seconds: Duration from creation to now
       - time_in_current_stage_seconds: Duration in current stage
       - time_to_success_seconds: Duration to reach SUCCESS (NULL if not successful)

    Example use cases:
    - Dashboard showing promotion success rate (excluding Alpha testing churn)
    - Alerting on production failures (PRODUCTION_FAILURE outcomes)
    - Analyzing promotion duration by outcome
    - Identifying stalled promotions (high record_count in IN_PROGRESS)

    Downstream aggregate table (agg_signal_promotion_daily_metrics) computes
    30-day rolling success rates by aggregating this snapshot over time.
    """
    return format_query(QUERY)
