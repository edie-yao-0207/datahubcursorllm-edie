"""
CAN Trace Training Candidates by Stream ID

Provides per-stream_id training candidate selection for ML-based signal reverse engineering.
Targets underrepresented stream_ids to build balanced training datasets.

This table uses the pre-computed `agg_mmyef_stream_ids` snapshot which provides
cumulative all-time trace counts per stream_id up to each date, enabling efficient
queries and supporting backfilling without lookback window issues.

Logic:
1. Get eligible traces from fct_eligible_traces_base (5-15 min, on-trip, valid MMY)
2. Join with agg_mmyef_stream_ids to get stream_ids per MMYEF
3. Count existing decoded traces per stream_id from agg_mmyef_stream_ids (cumulative all-time counts)
4. Identify insufficient stream_ids (< N decoded traces per stream_id)
5. Rank traces within each stream_id
6. Output top N per stream_id (default N=200)

The stream_id existence in agg_mmyef_stream_ids is sufficient to know signal definitions
exist - no additional coverage check is needed.

Key outputs:
- One row per (trace, stream_id) combination
- stream_id: The specific CAN stream to collect
- obd_value: Signal type (odometer=1, fuel_level=5, engine_seconds=157, etc.)
- stream_id_rank: Rank within this stream_id (1 = best)
- global_trace_rank: Global rank for quota enforcement
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
    get_monitored_metric_names_sql,
)
from dataclasses import replace

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
    CAPTURE_DURATION_COLUMN,
    END_TIME_COLUMN,
    START_TIME_COLUMN,
)


QUERY = """
WITH
-- Get monitored metrics from metadata table (type and obd_value mapping)
-- These are the signal types we monitor for coverage gaps
monitored_metrics AS (
    SELECT type, obd_value
    FROM {product_analytics_staging}.fct_telematics_stat_metadata
    WHERE type IN ({monitored_metric_names})
      AND obd_value IS NOT NULL
),

-- Use fct_eligible_traces_base which already:
-- - Joins eligible recording windows with vehicle properties
-- - Excludes already-collected traces (anti-join with fct_can_trace_status)
-- - Excludes internal orgs
-- Filter for on-trip traces with 5-15 minute duration
eligible_windows AS (
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
      AND etb.is_valid_mmy = TRUE
      AND etb.mmyef_id IS NOT NULL
      AND etb.on_trip = TRUE
      AND etb.capture_duration BETWEEN {min_duration_ms} AND {max_duration_ms}
),

-- Join with agg_mmyef_stream_ids to get stream_ids per MMYEF
-- Filter to only monitored signal types (same as reverse engineering candidates)
-- This is the key optimization: uses pre-computed snapshot instead of scanning map_can_classifications
-- Stream_id existence in the snapshot already proves signal definitions exist
traces_with_stream_ids AS (
    SELECT
        ew.date,
        ew.org_id,
        ew.device_id,
        ew.start_time,
        ew.end_time,
        ew.capture_duration,
        ew.mmyef_id,
        ams.stream_id,
        ams.obd_value,
        ams.trace_count AS historical_trace_count
    FROM eligible_windows ew
    JOIN {product_analytics_staging}.agg_mmyef_stream_ids ams USING (date, mmyef_id)
    JOIN monitored_metrics mm ON ams.obd_value = mm.obd_value
),

-- Count existing decoded traces per (date, stream_id)
-- This determines which stream_ids are underrepresented for each partition date
-- Uses agg_mmyef_stream_ids snapshot which has cumulative trace_count up to each date
-- Sum across MMYEFs to get total counts per stream_id (backfill-consistent)
existing_stream_id_counts AS (
    SELECT
        ams.date,
        ams.stream_id,
        SUM(ams.trace_count) AS existing_decoded_count
    FROM {product_analytics_staging}.agg_mmyef_stream_ids ams
    WHERE ams.date BETWEEN '{date_start}' AND '{date_end}'
    GROUP BY ams.date, ams.stream_id
),

-- Identify insufficient stream_ids (less than N decoded traces) per date
insufficient_stream_ids AS (
    SELECT
        tws.date,
        tws.stream_id,
        tws.obd_value,
        COALESCE(esc.existing_decoded_count, 0) AS existing_decoded_count,
        {traces_per_stream_id} - COALESCE(esc.existing_decoded_count, 0) AS needed_count
    FROM (SELECT DISTINCT date, stream_id, obd_value FROM traces_with_stream_ids) tws
    LEFT JOIN existing_stream_id_counts esc USING (date, stream_id)
    WHERE COALESCE(esc.existing_decoded_count, 0) < {traces_per_stream_id}
),

-- Get traces for insufficient stream_ids
training_candidates AS (
    SELECT
        tws.date,
        tws.org_id,
        tws.device_id,
        tws.start_time,
        tws.end_time,
        tws.capture_duration,
        tws.mmyef_id,
        tws.stream_id,
        tws.obd_value,
        iss.existing_decoded_count,
        iss.needed_count
    FROM traces_with_stream_ids tws
    JOIN insufficient_stream_ids iss USING (date, stream_id, obd_value)
),

-- Rank traces within each (date, stream_id)
-- Prioritize by capture_duration (longer = more data), then by recency (start_time DESC)
-- Note: Partition includes date to match per-date needed_count semantics
ranked_by_stream_id AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time,
        end_time,
        capture_duration,
        mmyef_id,
        stream_id,
        obd_value,
        existing_decoded_count,
        needed_count,
        ROW_NUMBER() OVER (
            PARTITION BY date, stream_id
            ORDER BY capture_duration DESC, start_time DESC
        ) AS stream_id_rank
    FROM training_candidates
),

-- Select top N per stream_id
top_per_stream_id AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time,
        end_time,
        capture_duration,
        mmyef_id,
        stream_id,
        obd_value,
        stream_id_rank,
        -- Global rank for quota enforcement at the final output
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY needed_count DESC, stream_id_rank ASC
        ) AS global_trace_rank
    FROM ranked_by_stream_id
    WHERE stream_id_rank <= needed_count  -- Only collect up to needed_count per stream_id
),

-- Final deduplication to ensure primary key uniqueness
-- Required because: a single stream_id can have multiple obd_values in agg_mmyef_stream_ids.
-- If the same trace (same date, org_id, device_id, start_time, end_time) appears with the same
-- stream_id but different obd_values, and multiple of those (stream_id, obd_value) combinations
-- are insufficient, the join with insufficient_stream_ids creates multiple rows for the same
-- primary key combination. Since the primary key is (date, org_id, device_id, start_time, end_time, stream_id)
-- and does NOT include obd_value, we need to deduplicate and keep one row per primary key.
deduplicated AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time,
        end_time,
        capture_duration,
        stream_id,
        obd_value,
        stream_id_rank,
        global_trace_rank,
        ROW_NUMBER() OVER (
            PARTITION BY date, org_id, device_id, start_time, end_time, stream_id
            ORDER BY global_trace_rank ASC, stream_id_rank ASC
        ) AS dedup_rank
    FROM top_per_stream_id
)

SELECT
    date,
    org_id,
    device_id,
    start_time,
    end_time,
    capture_duration,
    stream_id,
    obd_value,
    stream_id_rank,
    global_trace_rank
FROM deduplicated
WHERE dedup_rank = 1
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
    # Stream ID details
    replace(ColumnType.STREAM_ID.value, nullable=False, primary_key=True),
    replace(ColumnType.OBD_VALUE.value, nullable=False),
    # Ranking
    Column(
        name="stream_id_rank",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(
            comment="Rank within this stream_id (1 = best/highest priority). "
            "Lower rank = higher priority for collection."
        ),
    ),
    Column(
        name="global_trace_rank",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(
            comment="Per-date trace rank starting at 1 for each date. "
            "Ranked across all stream_ids within each date partition. "
            "Ordered by needed_count DESC (prioritize more underrepresented), then stream_id_rank ASC."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily per-stream_id training candidates for ML-based signal reverse engineering. "
        "Identifies underrepresented stream_ids (fewer than N decoded traces) and selects up to N "
        "traces per stream_id to build balanced training datasets. Uses agg_mmyef_stream_ids snapshot "
        "for efficient stream_id lookup - the existence of stream_ids in the snapshot already proves "
        "signal definitions exist, so no additional coverage check is needed. "
        "Supports backfilling as there is no lookback window dependency on historical trace data.",
        row_meaning="A ranked training candidate for a specific stream_id",
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
        AnyUpstream(ProductAnalyticsStaging.AGG_MMYEF_STREAM_IDS),
        AnyUpstream(ProductAnalyticsStaging.AGG_MMYEF_STREAM_IDS_DAILY),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_STAT_METADATA),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CAN_TRACE_TRAINING_BY_STREAM_ID.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
        include_empty_check=False,  # Table can be empty if all stream_ids have sufficient coverage
    ),
)
def fct_can_trace_training_by_stream_id(context: AssetExecutionContext) -> str:
    """
    Generate per-stream_id training candidates for ML reverse engineering.

    This asset provides targeted training data collection by:
    - Identifying underrepresented stream_ids (< N decoded traces)
    - Using pre-computed agg_mmyef_stream_ids for efficient stream_id lookup
    - Selecting up to N traces per stream_id to balance the training dataset

    Key features:
    - Per-stream_id granularity (vs MMYEF-level in reverse_engineering_candidates)
    - Stream_id existence in snapshot proves signal definitions exist
    - No lookback window dependency = supports backfilling
    - Includes stream_id and obd_value in output for downstream processing

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
        traces_per_stream_id=200,  # Default quota for per-stream_id training
        monitored_metric_names=get_monitored_metric_names_sql(),
    )
