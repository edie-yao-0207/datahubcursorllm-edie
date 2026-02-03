"""
CAN Trace Representative Candidates

Provides ranked CAN traces to collect for representative dataset per MMYEF population.

Key Features:
- One trace per device per day (randomly selected)
- Prefers traces with 5-15 minute duration (optimal quality window)
- Only includes on-trip traces
- Excludes traces already collected (via base table)
- Excludes MMYEFs that have already met their quota (default: 200 traces per MMYEF)
- External orgs only

Tag: can-set-main-0

Processing Logic:
1. Start from fct_eligible_traces_base (already filtered for eligible traces)
2. Filter for duration 5-15 minutes and on-trip
3. Select one random trace per device
4. Join with agg_tags_per_mmyef to check existing coverage
5. Exclude MMYEFs that have met their quota
6. Rank MMYEFs by collected_trace_count (fewer collected = higher priority), then by device_count (larger populations first)
7. Final ranking interleaves MMYEFs by their rank
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
from dataweb.userpkgs.firmware.can_trace_collection import (
    DURATION_5_TO_15_MIN,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

from .fct_can_recording_windows import (
    START_TIME_COLUMN,
    END_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
)

# Quota constant for representative candidates
TRACES_PER_MMYEF = 200

QUERY = """
WITH
-- Use fct_eligible_traces_base which already:
-- - Joins eligible recording windows with vehicle properties
-- - Excludes already-collected traces (anti-join with fct_can_trace_status)
-- - Excludes internal orgs
-- Filter for representative criteria: on-trip, 5-15 minute duration, valid MMY
filtered_eligible_traces AS (
    SELECT
        etb.date,
        etb.org_id,
        etb.device_id,
        etb.start_time,
        etb.end_time,
        etb.capture_duration,
        etb.mmyef_id
    FROM {product_analytics_staging}.fct_eligible_traces_base etb
    WHERE etb.date BETWEEN '{date_start}' AND '{date_end}'
      AND etb.on_trip = TRUE
      AND etb.capture_duration BETWEEN {min_duration_ms} AND {max_duration_ms}
      AND etb.is_valid_mmy = TRUE
      AND etb.mmyef_id IS NOT NULL
),

-- Select one random trace per device per day to increase diversity
-- and avoid overloading a VG with requests
one_trace_per_device AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time,
        end_time,
        capture_duration,
        mmyef_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY date, org_id, device_id
                ORDER BY RAND()
            ) as device_rank
        FROM filtered_eligible_traces
    )
    WHERE device_rank = 1
),

-- Join with collected trace counts per MMYEF per date
-- from agg_tags_per_mmyef (cumulative counts up to partition date)
-- Use tag-specific count for 'can-set-main-0' to enforce quota per tag
-- This ensures MMYEFs aren't excluded if they have 200+ traces under other tags (e.g., training)
traces_with_counts AS (
    SELECT
        otpd.*,
        COALESCE(atpm.tag_counts_map['can-set-main-0'], 0) AS collected_trace_count
    FROM one_trace_per_device otpd
    LEFT JOIN {product_analytics_staging}.agg_tags_per_mmyef atpm
        ON otpd.date = atpm.date
        AND otpd.mmyef_id = atpm.mmyef_id
),

-- Filter out traces from MMYEFs that have already met quota
unfilled_traces AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time,
        end_time,
        capture_duration,
        mmyef_id,
        collected_trace_count
    FROM traces_with_counts
    WHERE collected_trace_count < {traces_per_mmyef}
),

-- MMYEF-level ranking: prioritize MMYEFs with fewer collected traces, then by population size
-- This ensures MMYEFs with 0 collected traces are ranked higher than those with 9
-- Within the same collected count, larger populations (more devices) are ranked higher
mmyef_rank_per_date AS (
    SELECT DISTINCT
        ut.date,
        ut.mmyef_id,
        MIN(ut.collected_trace_count) AS mmyef_collected_count,
        MAX(COALESCE(mmyef_char.device_count, 0)) AS device_count,
        ROW_NUMBER() OVER (
            PARTITION BY ut.date
            ORDER BY MIN(ut.collected_trace_count) ASC,  -- Fewer collected = higher priority
                     MAX(COALESCE(mmyef_char.device_count, 0)) DESC,  -- Larger populations first
                     ut.mmyef_id  -- Determinism
        ) AS mmyef_rank
    FROM unfilled_traces ut
    LEFT JOIN {product_analytics_staging}.dim_mmyef_vehicle_characteristics mmyef_char
        ON ut.date = mmyef_char.date
        AND ut.mmyef_id = mmyef_char.mmyef_id
    GROUP BY ut.date, ut.mmyef_id
),

-- Final ranking: interleave MMYEFs by their rank (devices/traces already handled by one_trace_per_device)
-- Lower mmyef_rank (fewer collected traces) gets lower global_trace_rank (higher priority)
final_ranked_traces AS (
    SELECT
        ut.date,
        ut.org_id,
        ut.device_id,
        ut.start_time,
        ut.end_time,
        ut.capture_duration,
        ut.mmyef_id,
        ROW_NUMBER() OVER (
            PARTITION BY ut.date
            ORDER BY mr.mmyef_rank ASC,  -- MMYEFs with fewer collected traces first
                     ut.org_id,
                     ut.device_id
        ) AS global_trace_rank
    FROM unfilled_traces ut
    JOIN mmyef_rank_per_date mr USING(date, mmyef_id)
)

SELECT
    date,
    org_id,
    device_id,
    start_time,
    end_time,
    capture_duration,
    global_trace_rank
FROM final_ranked_traces
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
    Column(
        name="global_trace_rank",
        type=DataType.INTEGER,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Per-date trace rank starting at 1 for each date. "
            "Ranking prioritizes MMYEFs with fewer collected traces (collected_trace_count ASC), then by "
            "population size (device_count DESC) to prioritize larger populations within the same collected count. "
            "Lower ranks are higher priority for collection. MMYEFs with 0 collected traces are ranked higher than "
            "those with 199 collected traces, and among MMYEFs with the same collected count, larger populations are prioritized first."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily CAN trace candidates for representative dataset collection. "
        "Replaces the 'main-0' query from the Go service. Selects one random trace per device from "
        "eligible traces with 5-15 minute duration, on-trip, and excludes MMYEFs that have already "
        "met their quota (default: 200 traces per MMYEF). Uses fct_eligible_traces_base as the upstream "
        "source which handles external org filtering and already-collected trace exclusion. "
        "Ranking prioritizes MMYEFs with fewer collected traces (collected_trace_count ASC), then by "
        "population size (device_count DESC) to prioritize larger populations within the same collected count. "
        "This ensures MMYEFs with 0 collected traces are ranked higher than those with 9 collected traces, "
        "and among MMYEFs with the same collected count, larger populations are prioritized first.",
        row_meaning="A candidate CAN trace for representative dataset collection with MMYEF quota enforcement and intelligent ranking",
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
        AnyUpstream(ProductAnalyticsStaging.FCT_ELIGIBLE_TRACES_BASE),
        AnyUpstream(ProductAnalyticsStaging.AGG_TAGS_PER_MMYEF),
        AnyUpstream(ProductAnalyticsStaging.DIM_MMYEF_VEHICLE_CHARACTERISTICS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CAN_TRACE_REPRESENTATIVE_CANDIDATES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_can_trace_representative_candidates(context: AssetExecutionContext) -> str:
    """
    Generate candidate CAN traces for representative dataset collection.

    This asset replaces the 'main-0' query from the Go service (query.go).
    It identifies traces suitable for building a representative dataset
    per MMYEF population.

    Key criteria:
    - On-trip traces with 5-15 minute duration
    - One random trace per device for diversity
    - Excludes MMYEFs that have met their quota (default: 200)
    - Ranks MMYEFs by collected_trace_count (fewer collected = higher priority), then by device_count (larger populations first)
    - External orgs only (via base table)
    - Not already collected (via base table)

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    min_dur, max_dur = DURATION_5_TO_15_MIN
    return format_date_partition_query(
        QUERY,
        context,
        min_duration_ms=min_dur,
        max_duration_ms=max_dur,
        traces_per_mmyef=TRACES_PER_MMYEF,
    )

