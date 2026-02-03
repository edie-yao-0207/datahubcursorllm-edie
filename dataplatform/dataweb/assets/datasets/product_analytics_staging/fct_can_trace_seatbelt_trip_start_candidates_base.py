"""
CAN Trace Seatbelt Trip Start Candidates Base

Base table for seatbelt trip start event detection and classification.
Identifies CAN traces that capture first trip start events (with 1-minute pre-trip buffer)
and classifies them as training or inference candidates based on seatbelt event presence and coverage data.

Key Criteria:
- CAN recording starts at least 1 minute before first trip start
- First trip of day (6+ hours since last trip OR no previous trip) - cold start scenario preferred
- Full duration traces (>10 mins)
- Engine activity present (milliknots > 20k)

Classification:
- for_training: TRUE if seatbelt events observed during trace (ground truth available)
- for_inference: TRUE if no events AND no coverage AND eligible trace (coverage gap to fill)
- Excludes devices with coverage but no events (already handled)

Tag: can-set-seatbelt-trip-start-0
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
from dataweb.userpkgs.firmware.kinesisstats import KinesisStatsMetric
from dataweb.userpkgs.firmware.table import (
    DataModelTelematics,
    KinesisStats,
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

# Duration filter: 10+ minutes (600,000 ms)
MIN_DURATION_MS = 600000

# Time gap for "first trip of day" detection: 6 hours (21,600,000 ms)
FIRST_TRIP_GAP_MS = 21600000

# CAN recording must start at least 1 minute before trip start (60,000 ms)
TRIP_START_BUFFER_MS = 60000

# Engine milliknots threshold indicating vehicle is running
ENGINE_MILLIKNOTS_THRESHOLD = 20000

QUERY = """
WITH
-- Seatbelt buckle events (value = 1 means buckled)
-- Expand window by 1 day before to capture events during traces that span day boundaries
recent_seatbelt_buckled_events AS (
    SELECT
        sd.date,
        sd.time,
        sd.org_id,
        sd.object_id AS device_id,
        sd.value.int_value AS seatbelt_buckled
    FROM kinesisstats.osdseatbeltdriver sd
    WHERE sd.date BETWEEN DATE_SUB(TO_DATE('{date_start}'), 1) AND '{date_end}'
      AND sd.value.int_value = 1
),

-- Engine milliknots events
-- Expand window by 1 day before to capture events during traces that span day boundaries
recent_engine_milliknots AS (
    SELECT
        ecu.date,
        ecu.time,
        ecu.org_id,
        ecu.object_id AS device_id,
        ecu.value.int_value AS engine_milliknots
    FROM kinesisstats.osDEngineMilliknots ecu
    WHERE ecu.date BETWEEN DATE_SUB(TO_DATE('{date_start}'), 1) AND '{date_end}'
),

-- Engine milliknots > threshold (indicates vehicle is running)
recent_engine_milliknots_over_threshold AS (
    SELECT *
    FROM recent_engine_milliknots
    WHERE engine_milliknots > {engine_milliknots_threshold}
),

-- Trips with previous trip end time for "first trip" detection
-- Expand window by 1 day before to capture trips from previous day for gap detection
-- NOTE: LAG partitions by (org_id, device_id) only (not date) to allow cross-day gap detection
trips_with_previous AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time_ms,
        end_time_ms,
        LAG(end_time_ms) OVER (
            PARTITION BY org_id, device_id
            ORDER BY start_time_ms
        ) AS prev_end_time_ms
    FROM datamodel_telematics.fct_trips
    WHERE date BETWEEN DATE_SUB(TO_DATE('{date_start}'), 1) AND '{date_end}'
),

-- Eligible traces from base table with full duration filter
eligible_traces AS (
    SELECT
        etb.date,
        etb.org_id,
        etb.device_id,
        etb.start_time,
        etb.end_time,
        etb.capture_duration
    FROM {product_analytics_staging}.fct_eligible_traces_base etb
    WHERE etb.date BETWEEN '{date_start}' AND '{date_end}'
      AND etb.capture_duration > {min_duration_ms}  -- 10+ minutes
),

-- Join traces with trip/event data to detect trip starts
-- CAN recording must start at least 1 minute before trip start
-- Must be first trip of the day (6+ hours since last trip OR no previous trip)
augmented_traces AS (
    SELECT
        et.date,
        et.org_id,
        et.device_id,
        et.start_time,
        et.end_time,
        et.capture_duration,
        -- Did this trace capture a first trip start (with 1 min buffer)?
        MAX(CASE WHEN first_trip_start.device_id IS NOT NULL THEN 1 ELSE 0 END) AS first_trip_start,
        -- Did we see engine activity during this trace?
        MAX(CASE WHEN engine.device_id IS NOT NULL THEN 1 ELSE 0 END) AS has_engine_milliknots_over_threshold,
        -- Did we see seatbelt events during this trace?
        MAX(CASE WHEN seatbelt_event.device_id IS NOT NULL THEN 1 ELSE 0 END) AS has_seatbelt_event_during_trace
    FROM eligible_traces et
    -- Join to detect if trace captures a first trip start
    -- CAN recording starts at least 1 min before trip start and ends after trip starts
    -- Must be first trip of the day (6+ hours since last trip OR no previous trip)
    -- NOTE: No date equality - traces spanning midnight have date of end day, but trips/events
    -- from previous day are included in CTEs and matched via time-based conditions only
    LEFT JOIN trips_with_previous first_trip_start ON (
        et.org_id = first_trip_start.org_id
        AND et.device_id = first_trip_start.device_id
        AND first_trip_start.date >= DATE_SUB(et.date, 1) AND first_trip_start.date <= et.date  -- Restrict to previous day or same day
        AND et.start_time + {trip_start_buffer_ms} <= first_trip_start.start_time_ms  -- CAN starts at least 1 min before trip
        AND et.end_time > first_trip_start.start_time_ms  -- CAN ends after trip starts
        AND (first_trip_start.prev_end_time_ms IS NULL 
             OR (first_trip_start.start_time_ms - first_trip_start.prev_end_time_ms > {first_trip_gap_ms}))  -- First trip of day
    )
    -- Join to detect engine activity during trace
    -- NOTE: No date equality - allows matching events from previous day for traces spanning midnight
    LEFT JOIN recent_engine_milliknots_over_threshold engine ON (
        et.org_id = engine.org_id
        AND et.device_id = engine.device_id
        AND engine.date >= DATE_SUB(et.date, 1) AND engine.date <= et.date  -- Restrict to previous day or same day
        AND et.start_time < engine.time
        AND et.end_time > engine.time
    )
    -- Join to detect seatbelt events during trace
    -- NOTE: No date equality - allows matching events from previous day for traces spanning midnight
    LEFT JOIN recent_seatbelt_buckled_events seatbelt_event ON (
        et.org_id = seatbelt_event.org_id
        AND et.device_id = seatbelt_event.device_id
        AND seatbelt_event.date >= DATE_SUB(et.date, 1) AND seatbelt_event.date <= et.date  -- Restrict to previous day or same day
        AND et.start_time <= seatbelt_event.time
        AND et.end_time >= seatbelt_event.time
    )
    GROUP BY
        et.date,
        et.org_id,
        et.device_id,
        et.start_time,
        et.end_time,
        et.capture_duration
),

-- Get seatbelt coverage per device from coverage table
device_seatbelt_coverage AS (
    SELECT
        date,
        org_id,
        device_id,
        COALESCE(month_covered, FALSE) AS has_seatbelt_coverage
    FROM {product_analytics_staging}.fct_telematics_coverage_rollup_full
    WHERE date BETWEEN '{date_start}' AND '{date_end}'
      AND type = '{seatbelt_type}'
),

-- Filter for traces that capture trip starts and classify as training/inference
final_candidates AS (
    SELECT
        at.date,
        at.org_id,
        at.device_id,
        at.start_time,
        at.end_time,
        at.capture_duration,
        -- Classification flags:
        -- for_training: Events observed during trace (ground truth available)
        CAST(at.has_seatbelt_event_during_trace = 1 AS BOOLEAN) AS for_training,
        -- for_inference: No events, no coverage, but eligible trace (coverage gap to fill)
        CAST(
            at.has_seatbelt_event_during_trace = 0 
            AND COALESCE(dsc.has_seatbelt_coverage, FALSE) = FALSE
            AS BOOLEAN
        ) AS for_inference
    FROM augmented_traces at
    LEFT JOIN device_seatbelt_coverage dsc ON (
        at.date = dsc.date
        AND at.org_id = dsc.org_id
        AND at.device_id = dsc.device_id
    )
    WHERE at.first_trip_start = 1  -- Must capture a first trip start (with 1 min buffer)
      AND at.has_engine_milliknots_over_threshold = 1  -- Engine must be running (milliknots > 20k)
      AND (
          -- Include training candidates (events present)
          at.has_seatbelt_event_during_trace = 1
          OR
          -- Include inference candidates (no events, no coverage)
          (at.has_seatbelt_event_during_trace = 0 AND COALESCE(dsc.has_seatbelt_coverage, FALSE) = FALSE)
          -- Exclude: devices with coverage but no events (already handled)
      )
)

SELECT
    date,
    org_id,
    device_id,
    start_time,
    end_time,
    capture_duration,
    for_training,
    for_inference
FROM final_candidates
ORDER BY date, org_id, device_id, start_time
"""

COLUMNS = [
    ColumnType.DATE,
    # Device identifiers
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
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Base table for seatbelt trip start event detection and classification. "
        "Identifies CAN traces that capture first trip start events (with 1-minute pre-trip buffer) "
        "and classifies them as training or inference candidates based on seatbelt event presence and coverage data. "
        "Training candidates have observed events (ground truth), while inference candidates have coverage gaps "
        "(no events, no coverage). This base table provides the domain logic for event detection and classification; "
        "downstream ranking tables add prioritization logic.",
        row_meaning="A candidate CAN trace that captures a first trip start event (with 1-minute pre-trip buffer), "
        "classified as training (events present) or inference (coverage gap) candidate for seatbelt signal collection",
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
        AnyUpstream(KinesisStats.OSD_SEAT_BELT_DRIVER),
        AnyUpstream(KinesisStats.OSD_ENGINE_MILLI_KNOTS),
        AnyUpstream(DataModelTelematics.FCT_TRIPS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CAN_TRACE_SEATBELT_TRIP_START_CANDIDATES_BASE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_can_trace_seatbelt_trip_start_candidates_base(context: AssetExecutionContext) -> str:
    """
    Generate base candidate CAN traces for seatbelt trip start collection (event detection and classification).

    This asset identifies traces that capture first trip start events (with 1-minute pre-trip buffer)
    and classifies them as training or inference candidates based on seatbelt event presence and coverage data.

    Key criteria:
    - CAN recording starts at least 1 minute before first trip start
    - Full duration traces (>10 mins)
    - First trip of the day captured (6+ hours since last trip OR no previous trip)
    - Engine activity present (milliknots > 20k)

    Classification:
    - for_training: TRUE if seatbelt events observed during trace (ground truth available)
    - for_inference: TRUE if no events AND no coverage AND eligible trace (coverage gap to fill)
    - Excludes devices with coverage but no events (already handled, don't need more data)

    This base table provides event detection and classification logic only.
    Downstream ranking tables (e.g., fct_can_trace_seatbelt_trip_start_candidates) add
    prioritization and ranking logic.

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    return format_date_partition_query(
        QUERY,
        context,
        min_duration_ms=MIN_DURATION_MS,
        first_trip_gap_ms=FIRST_TRIP_GAP_MS,
        trip_start_buffer_ms=TRIP_START_BUFFER_MS,
        engine_milliknots_threshold=ENGINE_MILLIKNOTS_THRESHOLD,
        seatbelt_type=KinesisStatsMetric.SEAT_BELT_DRIVER_INT_VALUE.label,
    )

