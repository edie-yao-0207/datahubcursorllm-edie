"""
CAN Trace Reverse Engineering Candidates

Provides ranked CAN traces to collect for ML-based automated signal reverse engineering.
This unified table serves both training and inference needs by identifying traces from
devices with telematics coverage gaps.

Logic:
1. Get eligible traces from fct_eligible_traces_base (5-15 min, on-trip, valid MMY)
2. Compute device-level coverage (coverage is per-device, not per-trace)
3. Identify devices with at least one coverage gap
4. Filter to MMYEFs that have at least one device with gaps (MMYEF-level prioritization)
5. Rank using three-level approach: MMYEF rank → device rank (depth first) → trace rank
   - Depth-first device ranking prioritizes devices with more collected traces to allow models to work sooner
   - Training candidates (more covered signals) prioritized before inference candidates (more gaps)
     since inference requires training data first
   - Note: fct_can_trace_representative_candidates already handles device diversity
6. Derive boolean flags based on device-level coverage:
   - for_training: TRUE if device has at least one signal WITH coverage (ground truth data available)
   - for_inference: TRUE if device has at least one signal WITHOUT coverage (always TRUE given filter)

Key outputs:
- One row per unique trace
- coverage_gap_count: how many signal types lack coverage for this device (1-7, device-level)
- covered_signal_count: how many signal types have coverage for this device (device-level)
- for_training: TRUE if device has covered signals (ground truth for ML training, device-level)
- for_inference: TRUE if device has coverage gaps (signals to reverse engineer, device-level)
- training_signal_types: Array of signal types with coverage for this device (device-level)
- inference_signal_types: Array of signal types without coverage for this device (device-level)
- mmyef_signal_populations_collected_count: Number of traces already collected for this MMYEF with 'can-set-signal-populations-0' tag (MMYEF-level)
- device_signal_populations_collected_count: Number of traces already collected for this device with 'can-set-signal-populations-0' tag (device-level)
- global_trace_rank: Per-date rank starting at 1, with depth-first device ranking (prioritizes devices with more collected traces)
- Device and timing details needed for collection targeting
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
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    array_of,
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
eligible_windows_with_mmyef AS (
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

-- Get distinct devices from eligible traces for device-level coverage computation
distinct_devices AS (
    SELECT DISTINCT
        date,
        org_id,
        device_id,
        mmyef_id
    FROM eligible_windows_with_mmyef
),

-- Compute device-level coverage per signal type
-- Coverage is per-device, not per-trace, so we compute once per device
device_coverage_per_signal AS (
    SELECT
        dd.date,
        dd.org_id,
        dd.device_id,
        dd.mmyef_id,
        mm.type,
        mm.obd_value,
        COALESCE(coverage.month_covered, FALSE) AS has_coverage
    FROM distinct_devices dd
    CROSS JOIN monitored_metrics mm
    LEFT JOIN product_analytics_staging.fct_telematics_coverage_rollup_full coverage
        ON dd.date = coverage.date
        AND dd.org_id = coverage.org_id
        AND dd.device_id = coverage.device_id
        AND mm.type = coverage.type
),

-- Aggregate to device-level coverage statistics
-- Count signals with coverage (for training) and signals without coverage (gaps for inference)
-- Also compute signal type arrays per device (merged from device_signal_arrays to eliminate duplicate scan)
device_coverage_stats AS (
    SELECT
        date,
        org_id,
        device_id,
        mmyef_id,
        COUNT(CASE WHEN has_coverage THEN 1 END) AS covered_signal_count,
        COUNT(CASE WHEN NOT has_coverage THEN 1 END) AS coverage_gap_count,
        COLLECT_SET(CASE WHEN has_coverage THEN obd_value ELSE NULL END) AS training_signal_types,
        COLLECT_SET(CASE WHEN NOT has_coverage THEN obd_value ELSE NULL END) AS inference_signal_types
    FROM device_coverage_per_signal
    GROUP BY date, org_id, device_id, mmyef_id
    HAVING coverage_gap_count > 0  -- At least one uncovered signal type (inference candidate)
),

-- Join eligible traces with device coverage stats (includes signal arrays)
-- The JOIN with device_coverage_stats already filters to only devices with coverage gaps,
-- which implicitly filters to only MMYEFs that have devices with gaps
traces_with_device_coverage AS (
    SELECT
        ew.date,
        ew.org_id,
        ew.device_id,
        ew.start_time,
        ew.end_time,
        ew.capture_duration,
        ew.mmyef_id,
        dcs.covered_signal_count,
        dcs.coverage_gap_count,
        dcs.training_signal_types,
        dcs.inference_signal_types
    FROM eligible_windows_with_mmyef ew
    JOIN device_coverage_stats dcs USING(date, org_id, device_id, mmyef_id)
),

-- Join with MMYEF-level collected counts (from agg_tags_per_mmyef)
-- and device-level collected counts (for depth-first ranking)
traces_with_all_counts AS (
    SELECT
        twdc.date,
        twdc.org_id,
        twdc.device_id,
        twdc.start_time,
        twdc.end_time,
        twdc.capture_duration,
        twdc.mmyef_id,
        twdc.covered_signal_count,
        twdc.coverage_gap_count,
        twdc.training_signal_types,
        twdc.inference_signal_types,
        -- Boolean flags derived from device-level coverage data:
        -- for_training: TRUE if device has at least one signal WITH coverage (ground truth to train on)
        (twdc.covered_signal_count > 0) AS for_training,
        -- for_inference: TRUE if device has at least one signal WITHOUT coverage (gap to fill)
        -- Note: Always TRUE here since we filter to coverage_gap_count > 0 above
        (twdc.coverage_gap_count > 0) AS for_inference,
        -- MMYEF-level tag-specific collected count (from agg_tags_per_mmyef)
        -- Use signal-populations-0 tag count for quota enforcement
        COALESCE(mmyef_tags.tag_counts_map['can-set-signal-populations-0'], 0) AS mmyef_signal_populations_collected_count,
        -- Device-level tag-specific collected count (for depth-first ranking, from agg_tags_per_device)
        -- Use signal-populations-0 tag count for depth-first device ranking (higher counts = higher priority)
        COALESCE(device_tags.tag_counts_map['can-set-signal-populations-0'], 0) AS device_signal_populations_collected_count
    FROM traces_with_device_coverage twdc
    LEFT JOIN {product_analytics_staging}.agg_tags_per_mmyef mmyef_tags
        ON twdc.date = mmyef_tags.date
        AND twdc.mmyef_id = mmyef_tags.mmyef_id
    LEFT JOIN {product_analytics_staging}.agg_tags_per_device device_tags
        ON twdc.date = device_tags.date
        AND twdc.org_id = device_tags.org_id
        AND twdc.device_id = device_tags.device_id
),

-- First level: Rank MMYEFs per date (for prioritization)
-- Prioritize MMYEFs with more aggregate coverage gaps and fewer collected traces
mmyef_rank_per_date AS (
    SELECT DISTINCT
        date,
        mmyef_id,
        SUM(coverage_gap_count) AS mmyef_total_coverage_gaps,
        MAX(mmyef_signal_populations_collected_count) AS mmyef_signal_populations_collected_count,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY SUM(coverage_gap_count) DESC,
                     MAX(mmyef_signal_populations_collected_count) ASC,
                     mmyef_id
        ) AS mmyef_rank
    FROM traces_with_all_counts
    GROUP BY date, mmyef_id
),

-- Second level: Rank devices within each MMYEF per date
-- Prioritize devices with MORE collected traces (depth first) to allow models to work sooner
-- Then prioritize training candidates (MORE coverage) before inference candidates (MORE gaps)
-- This ensures we fill the training set before inference set since inference requires training data
-- Note: fct_can_trace_representative_candidates already handles device diversity
-- First compute device-level ranks (one rank per device, not per trace)
-- Aggregate to device level first before applying ROW_NUMBER() to avoid cross-product
device_ranks AS (
    SELECT
        date,
        org_id,
        device_id,
        mmyef_id,
        MAX(covered_signal_count) AS covered_signal_count,
        MAX(coverage_gap_count) AS coverage_gap_count,
        MAX(training_signal_types) AS training_signal_types,
        MAX(inference_signal_types) AS inference_signal_types,
        MAX(for_training) AS for_training,
        MAX(for_inference) AS for_inference,
        MAX(mmyef_signal_populations_collected_count) AS mmyef_signal_populations_collected_count,
        MAX(device_signal_populations_collected_count) AS device_signal_populations_collected_count,
        ROW_NUMBER() OVER (
            PARTITION BY date, mmyef_id
            ORDER BY MAX(device_signal_populations_collected_count) DESC,  -- More collected = higher priority (depth first)
                     MAX(covered_signal_count) DESC,  -- More coverage = higher priority (training first)
                     MAX(coverage_gap_count) DESC,  -- More gaps = higher priority (inference second)
                     device_id
        ) AS device_rank_within_mmyef
    FROM traces_with_all_counts
    GROUP BY date, org_id, device_id, mmyef_id
),
-- Join device ranks back to traces so all traces from the same device share the same rank
device_rank_within_mmyef AS (
    SELECT
        twac.date,
        twac.org_id,
        twac.device_id,
        twac.mmyef_id,
        twac.start_time,
        twac.end_time,
        twac.capture_duration,
        twac.covered_signal_count,
        twac.coverage_gap_count,
        twac.training_signal_types,
        twac.inference_signal_types,
        twac.for_training,
        twac.for_inference,
        twac.mmyef_signal_populations_collected_count,
        twac.device_signal_populations_collected_count,
        dr.device_rank_within_mmyef
    FROM traces_with_all_counts twac
    JOIN device_ranks dr
        ON twac.date = dr.date
        AND twac.org_id = dr.org_id
        AND twac.device_id = dr.device_id
        AND twac.mmyef_id = dr.mmyef_id
),

-- Third level: Rank traces within each device
-- Prefer longer traces (higher capture_duration)
trace_rank_within_device AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time,
        end_time,
        capture_duration,
        mmyef_id,
        coverage_gap_count,
        training_signal_types,
        inference_signal_types,
        for_training,
        for_inference,
        mmyef_signal_populations_collected_count,
        device_signal_populations_collected_count,
        device_rank_within_mmyef,
        ROW_NUMBER() OVER (
            PARTITION BY date, org_id, device_id
            ORDER BY capture_duration DESC
        ) AS trace_rank_within_device
    FROM device_rank_within_mmyef
),

-- Final global ranking: interleave MMYEFs, then devices within MMYEF, then traces within device
ranked_traces AS (
    SELECT
        trwd.date,
        trwd.org_id,
        trwd.device_id,
        trwd.start_time,
        trwd.end_time,
        trwd.capture_duration,
        trwd.mmyef_id,
        trwd.coverage_gap_count,
        trwd.training_signal_types,
        trwd.inference_signal_types,
        trwd.for_training,
        trwd.for_inference,
        trwd.mmyef_signal_populations_collected_count,
        trwd.device_signal_populations_collected_count,
        ROW_NUMBER() OVER (
            PARTITION BY trwd.date
            ORDER BY mr.mmyef_rank ASC,
                     trwd.device_rank_within_mmyef ASC,
                     trwd.trace_rank_within_device ASC
        ) AS global_trace_rank
    FROM trace_rank_within_device trwd
    JOIN mmyef_rank_per_date mr USING(date, mmyef_id)
)

SELECT
    date,
    org_id,
    device_id,
    start_time,
    end_time,
    capture_duration,
    coverage_gap_count,
    for_training,
    for_inference,
    training_signal_types,
    inference_signal_types,
    mmyef_signal_populations_collected_count,
    device_signal_populations_collected_count,
    global_trace_rank
FROM ranked_traces
ORDER BY global_trace_rank ASC
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    START_TIME_COLUMN,
    END_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
    Column(
        name="coverage_gap_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of signal types (odometer, fuel_level_percent, engine_seconds, etc.) "
            "that lack telematics coverage for this device. Higher counts indicate more signals "
            "needing reverse engineering. Always >= 1. This is device-level (not MMYEF-level) since coverage is per-device."
        ),
    ),
    Column(
        name="for_training",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="TRUE if this device has at least one signal WITH telematics coverage. "
            "Traces with for_training=TRUE have ground truth data available for ML model training. "
            "FALSE if all monitored signals lack coverage (no ground truth to train on). "
            "This is device-level (not MMYEF-level) since coverage is per-device."
        ),
    ),
    Column(
        name="for_inference",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="TRUE if this device has at least one signal WITHOUT telematics coverage (gap). "
            "Traces with for_inference=TRUE are candidates for using ML models to reverse engineer "
            "unknown proprietary signals. Always TRUE in this table since we filter to coverage_gap_count > 0. "
            "This is device-level (not MMYEF-level) since coverage is per-device."
        ),
    ),
    Column(
        name="training_signal_types",
        type=array_of(DataType.LONG),
        nullable=True,
        metadata=Metadata(
            comment="Array of OBD_VALUEs (BIGINT) that have telematics coverage for this device "
            "(e.g., [1, 5, 157]). These OBD values correspond to signals that can be used for ML model training "
            "as they have ground truth data available. Empty array if no signals have coverage (for_training = FALSE). "
            "This is device-level (all traces from the same device have the same array) since coverage is per-device."
        ),
    ),
    Column(
        name="inference_signal_types",
        type=array_of(DataType.LONG),
        nullable=True,
        metadata=Metadata(
            comment="Array of OBD_VALUEs (BIGINT) that lack telematics coverage for this device "
            "(e.g., [157, 52]). These OBD values correspond to signals that are candidates for reverse engineering "
            "using ML models. Always non-empty in this table since we filter to devices with at least one coverage gap. "
            "This is device-level (all traces from the same device have the same array) since coverage is per-device."
        ),
    ),
    Column(
        name="mmyef_signal_populations_collected_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of traces already collected for this MMYEF with 'can-set-signal-populations-0' tag. "
            "Used for MMYEF-level diversity-aware ranking (lower counts = higher priority). "
            "Zero if no signal-populations traces collected yet. Tag-specific count ensures proper quota enforcement per tag."
        ),
    ),
    Column(
        name="device_signal_populations_collected_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of traces already collected for this device with 'can-set-signal-populations-0' tag. "
            "Used for device-level depth-first ranking (higher counts = higher priority) to allow models to work sooner. "
            "Zero if no signal-populations traces collected yet. Note: fct_can_trace_representative_candidates already handles device diversity. "
            "Tag-specific count ensures proper quota enforcement per tag."
        ),
    ),
    Column(
        name="global_trace_rank",
        type=DataType.INTEGER,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Per-date trace rank starting at 1 for each date. Three-level ranking: "
            "(1) MMYEF rank by aggregate coverage gaps and signal-populations tag count ASC (for population prioritization), "
            "(2) Device rank within MMYEF by signal-populations tag count DESC (depth first), then covered_signal_count DESC (training first), then coverage_gap_count DESC (inference second), "
            "(3) Trace rank within device by capture_duration DESC (prefer longer traces). "
            "Training candidates prioritized before inference candidates since inference requires training data. Note: fct_can_trace_representative_candidates already handles device diversity."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily recommendations of CAN traces to collect for ML-based automated signal reverse engineering. "
        "Unified table serving both training and inference needs. Identifies traces from vehicles (MMYEFs) that lack "
        "telematics coverage for monitored signal types (odometer, fuel_level_percent, engine_seconds, etc.). "
        "Uses fct_telematics_coverage_rollup_full to identify coverage gaps. "
        "Ranking uses per-date three-level approach: (1) MMYEF rank by aggregate coverage gaps and collected counts, "
        "(2) Device rank within MMYEF by collected counts DESC (depth first), then covered_signal_count DESC (training first), "
        "then coverage_gap_count DESC (inference second), (3) Trace rank within device by capture_duration. "
        "Training candidates prioritized before inference candidates since inference requires training data. "
        "Depth-first device ranking prioritizes devices with more collected traces to allow models to work sooner. "
        "Note: fct_can_trace_representative_candidates already handles device diversity. "
        "Global trace rank starts at 1 for each date. "
        "Boolean flags derived from actual coverage: for_training=TRUE if device has at least one covered signal "
        "(ground truth available), for_inference=TRUE if device has at least one gap (always TRUE given filter). "
        "Signal type arrays (training_signal_types, inference_signal_types) explicitly list which signal types "
        "can be used for training vs inference per device. "
        "Tag-specific collected trace counts (mmyef_signal_populations_collected_count, device_signal_populations_collected_count) "
        "are included for ranking and decision-making. "
        "Includes device identifiers and timing windows needed for collection targeting.",
        row_meaning="A ranked CAN trace recommendation for reverse engineering, with flags indicating training/inference candidacy and collected counts for ranking",
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
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP_FULL),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_STAT_METADATA),
        AnyUpstream(ProductAnalyticsStaging.AGG_TAGS_PER_MMYEF),
        AnyUpstream(ProductAnalyticsStaging.AGG_TAGS_PER_DEVICE),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CAN_TRACE_REVERSE_ENGINEERING_CANDIDATES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_can_trace_reverse_engineering_candidates(context: AssetExecutionContext) -> str:
    """
    Generate ranked CAN traces for reverse engineering model training and inference.

    This unified asset provides device-level coverage analysis and three-level ranking
    for balanced collection across MMYEF populations and devices.

    Key features:
    - Computes coverage at device-level (not per-trace) since coverage is per-device
    - Identifies devices with at least one telematics coverage gap
    - Filters to MMYEFs that have at least one device with gaps (MMYEF-level prioritization)
    - Per-date three-level ranking:
      (1) MMYEF rank by aggregate coverage gaps and MMYEF collected counts
      (2) Device rank within MMYEF by device collected counts (depth first), then covered_signal_count (training first),
          then coverage_gap_count (inference second)
      (3) Trace rank within device by capture_duration (prefer longer traces)
    - Training candidates prioritized before inference candidates since inference requires training data
    - Depth-first device ranking prioritizes devices with more collected traces to allow models to work sooner
    - Note: fct_can_trace_representative_candidates already handles device diversity
    - Derives boolean flags from device-level coverage data:
      - for_training: TRUE if device has at least one covered signal (ground truth)
      - for_inference: TRUE if device has at least one gap (always TRUE here)
    - Provides signal type arrays per device (all traces from same device have same arrays)
    - Includes MMYEF-level tag-specific collected counts from agg_tags_per_mmyef (can-set-signal-populations-0) for MMYEF diversity
    - Includes device-level tag-specific collected counts from agg_tags_per_device (can-set-signal-populations-0) for depth-first ranking
    - No lookback window dependency = supports backfilling

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
        monitored_metric_names=get_monitored_metric_names_sql(),
    )

