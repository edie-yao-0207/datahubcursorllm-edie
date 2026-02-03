"""
CAN Trace Seatbelt Trip Start Candidates

Provides ranked CAN traces for seatbelt trip start collection.
Reads from fct_can_trace_seatbelt_trip_start_candidates_base (event detection and classification)
and adds MMYEF-level diversity-aware ranking to prioritize traces for collection.

Ranking Strategy:
- MMYEF-level ranking prioritizes MMYEFs with fewer collected traces first
- This ensures we build up enough samples per MMYEF for effective ML training/inference
- before moving to the next MMYEF (better than pulling one trace from each MMYEF)
- Uses agg_tags_per_mmyef.tag_counts_map['can-set-seatbelt-trip-start-0'] to track cumulative counts
- Ranking is deterministic (no randomness) for reproducible results across backfills
- Traces within the same MMYEF are ranked deterministically by start_time, then device_id

Tag: can-set-seatbelt-trip-start-0

Processing Logic:
1. Read candidate traces from base table (event detection and classification already done)
2. Join with vehicle properties to get MMYEF for diversity tracking
3. Join with collected trace counts per MMYEF
4. Rank MMYEFs by collected counts (fewer collected = higher priority) to build up samples per MMYEF
5. Final global ranking interleaves MMYEFs by their rank

"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    AWSRegion,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    DQCheckMode,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

from .fct_can_recording_windows import (
    START_TIME_COLUMN,
    END_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
)

QUERY = """
WITH
-- Read from base table (event detection and classification)
base_candidates AS (
    SELECT *
    FROM {product_analytics_staging}.fct_can_trace_seatbelt_trip_start_candidates_base
    WHERE date BETWEEN '{date_start}' AND '{date_end}'
),

-- Join with vehicle properties to get MMYEF for diversity tracking
candidates_with_mmyef AS (
    SELECT
        bc.*,
        dvp.mmyef_id
    FROM base_candidates bc
    LEFT JOIN {product_analytics_staging}.dim_device_vehicle_properties dvp
        ON bc.date = dvp.date
        AND bc.org_id = dvp.org_id
        AND bc.device_id = dvp.device_id
),

-- Join with collected trace counts per MMYEF per date
-- from agg_tags_per_mmyef (cumulative counts up to partition date)
-- Extract tag-specific count for 'can-set-seatbelt-trip-start-0' from tag_counts_map
traces_with_counts AS (
    SELECT
        cwm.*,
        COALESCE(mmyef_tags.tag_counts_map['can-set-seatbelt-trip-start-0'], 0) AS mmyef_total_collected_count
    FROM candidates_with_mmyef cwm
    LEFT JOIN {product_analytics_staging}.agg_tags_per_mmyef mmyef_tags
        ON cwm.date = mmyef_tags.date
        AND cwm.mmyef_id = mmyef_tags.mmyef_id
),

-- MMYEF-level ranking: prioritize MMYEFs with fewer collected traces
-- This ensures we build up enough samples per MMYEF for effective ML training/inference
-- before moving to the next MMYEF (MMYEFs with 0 collected traces are ranked higher than those with 9)
mmyef_rank_per_date AS (
    SELECT DISTINCT
        date,
        mmyef_id,
        MIN(mmyef_total_collected_count) AS mmyef_collected_count,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY MIN(mmyef_total_collected_count) ASC,  -- Fewer collected = higher priority (build up samples per MMYEF)
                     mmyef_id  -- Determinism
        ) AS mmyef_rank
    FROM traces_with_counts
    WHERE mmyef_id IS NOT NULL
    GROUP BY date, mmyef_id
),

-- Final ranking: interleave MMYEFs by their rank, then deterministic within MMYEF
ranked_traces AS (
    SELECT
        twc.date,
        twc.org_id,
        twc.device_id,
        twc.start_time,
        twc.end_time,
        twc.capture_duration,
        twc.for_training,
        twc.for_inference,
        ROW_NUMBER() OVER (
            PARTITION BY twc.date
            ORDER BY COALESCE(mr.mmyef_rank, 999999) ASC,  -- MMYEFs with fewer collected traces first (build up samples per MMYEF, NULLs last)
                     twc.start_time ASC,  -- Deterministic within MMYEF
                     twc.device_id  -- Additional determinism
        ) AS global_trace_rank
    FROM traces_with_counts twc
    LEFT JOIN mmyef_rank_per_date mr
        ON twc.date = mr.date
        AND twc.mmyef_id = mr.mmyef_id
)

SELECT
    date,
    org_id,
    device_id,
    start_time,
    end_time,
    capture_duration,
    for_training,
    for_inference,
    global_trace_rank
FROM ranked_traces
ORDER BY global_trace_rank ASC
"""

COLUMNS = [
    ColumnType.DATE,
    # Device identifiers - join dim_device_vehicle_properties on (date, org_id, device_id) for vehicle details
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    # Trace timing details
    START_TIME_COLUMN,
    END_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
    # Classification flags
    Column(
        name="for_training",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="TRUE if seatbelt events were observed during this trace. "
            "Traces with for_training=TRUE have ground truth data available for ML model training."
        ),
    ),
    Column(
        name="for_inference",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="TRUE if no seatbelt events were observed AND device has no seatbelt coverage. "
            "Traces with for_inference=TRUE are candidates for using ML models to reverse engineer "
            "seatbelt signals from CAN data (coverage gap to fill)."
        ),
    ),
    # Ranking
    Column(
        name="global_trace_rank",
        type=DataType.INTEGER,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Global rank across all candidate traces for this date. "
            "Ranking prioritizes MMYEFs with fewer collected traces first to build up enough samples "
            "per MMYEF for effective ML training/inference before moving to the next MMYEF. "
            "Lower ranks are higher priority for collection. Ranking is deterministic (no randomness)."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily ranked CAN trace candidates for seatbelt trip start collection. "
        "Reads from fct_can_trace_seatbelt_trip_start_candidates_base (event detection and classification) "
        "and adds MMYEF-level ranking to prioritize traces for collection. "
        "Ranking prioritizes MMYEFs with fewer collected traces first to build up enough samples per MMYEF "
        "for effective ML training/inference before moving to the next MMYEF. "
        "Uses tag-specific counts from agg_tags_per_mmyef.tag_counts_map['can-set-seatbelt-trip-start-0'].",
        row_meaning="A ranked candidate CAN trace for seatbelt trip start collection with diversity-aware prioritization",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_CAN_TRACE_SEATBELT_TRIP_START_CANDIDATES_BASE),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
        AnyUpstream(ProductAnalyticsStaging.AGG_TAGS_PER_MMYEF),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CAN_TRACE_SEATBELT_TRIP_START_CANDIDATES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_can_trace_seatbelt_trip_start_candidates(context: AssetExecutionContext) -> str:
    """
    Generate ranked candidate CAN traces for seatbelt trip start collection.

    This asset reads from fct_can_trace_seatbelt_trip_start_candidates_base (which handles
    event detection and classification) and adds MMYEF-level diversity-aware ranking to
    prioritize traces for collection.

    Ranking Strategy:
    - MMYEF-level ranking prioritizes MMYEFs with fewer collected traces first
    - This ensures we build up enough samples per MMYEF for effective ML training/inference
    - before moving to the next MMYEF (better than pulling one trace from each MMYEF)
    - Uses agg_tags_per_mmyef.tag_counts_map['can-set-seatbelt-trip-start-0'] to track cumulative counts
    - Ranking is deterministic (no randomness) for reproducible results across backfills
    - Traces within the same MMYEF are ranked deterministically by start_time, then device_id

    Output includes:
    - All columns from base table (date, org_id, device_id, start_time, end_time, capture_duration, for_training, for_inference)
    - global_trace_rank: Diversity-aware rank for collection prioritization

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    return format_date_partition_query(QUERY, context)

