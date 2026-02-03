"""
Per-device installation recommendations for VG devices based on binary signal detection

Stores device-level binary detection flags (TRUE if signal detected in last month, FALSE/NULL if not)
and recommendations (Plug & Play if signal present, Hard-Wired Required if not) for AIM4 features
(turn signal, reverse gear, door status). Provides the granular fact data that feeds aggregate
install recommendation summaries.

Each firmware version struct contains the expected coverage at that release level:
- pre_vg36_fw: Pre-VG36 baseline (j1939_oel_turn_signal_switch_state, vehicle_current_gear)
- vg36_fw: VG-36.1.0 (rollout completes Dec 18, 2025) - adds SPN 2367 (left turn signal) and
  SPN 2369 (right turn signal) support for octo trigger logic input
- vg37_fw: VG-37 target - adds SPN 767 (reverse direction) and SPN 1619 (direction indicator)
  for improved reverse gear detection
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

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
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    struct_from_columns,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.kinesisstats import KinesisStatsMetric


class InstallRecommendation(Enum):
    """
    Installation recommendation levels with associated coverage thresholds.

    Each level has a label (for display) and a threshold (minimum coverage ratio 0.0-1.0
    to qualify for this recommendation at the population level).

    Device-level uses binary: PLUG_AND_PLAY (signal detected) or HARD_WIRED_REQUIRED (not detected).
    Population-level uses all four with graduated thresholds.
    """

    # (label, threshold) - threshold is minimum coverage ratio to qualify
    PLUG_AND_PLAY = ("Plug & Play", 0.96)
    LIKELY_PLUG_AND_PLAY = ("Likely Plug & Play", 0.50)
    LIKELY_HARD_WIRED = ("Likely Hard-Wired", 0.01)
    HARD_WIRED_REQUIRED = ("Hard-Wired Required", 0.0)

    @property
    def label(self) -> str:
        """The display label for this recommendation."""
        return self.value[0]

    @property
    def threshold(self) -> float:
        """Minimum coverage ratio (0.0-1.0) to qualify for this recommendation."""
        return self.value[1]


@dataclass(frozen=True)
class SignalConfig:
    """Configuration for a signal type with current and future signal definitions.

    Attributes:
        current: The current signal metric (pre-VG36 baseline)
        future: List of future signal metrics that will be available in vg_release
        vg_release: Firmware version where future signals become available (e.g., "VG-36")
    """

    current: KinesisStatsMetric
    future: List[KinesisStatsMetric]
    vg_release: Optional[str] = None

    @property
    def current_label(self) -> str:
        """Label for the current signal."""
        return self.current.label

    @property
    def future_labels(self) -> List[str]:
        """Labels for all future signals."""
        return [sig.label for sig in self.future]

    @property
    def all_labels(self) -> List[str]:
        """Labels for current + all future signals."""
        return [self.current_label] + self.future_labels


@dataclass(frozen=True)
class SignalConfigs:
    """All signal configurations for install recommendations.

    Each signal may have multiple candidate metrics; coverage uses the best available.
    """

    turn_signal: SignalConfig
    reverse_gear: SignalConfig
    door_status: SignalConfig
    can_connected: SignalConfig


# Signal type configurations for multi-signal coverage
SIGNAL_CONFIGS = SignalConfigs(
    turn_signal=SignalConfig(
        current=KinesisStatsMetric.J1939_OEL_TURN_SIGNAL_SWITCH_STATE_INT_VALUE,
        future=[
            KinesisStatsMetric.J1939_LCMD_LEFT_TURN_SIGNAL_LIGHTS_COMMAND_STATE_INT_VALUE,
            KinesisStatsMetric.J1939_LCMD_RIGHT_TURN_SIGNAL_LIGHTS_COMMAND_STATE_INT_VALUE,
        ],
        vg_release="VG-36",
    ),
    reverse_gear=SignalConfig(
        current=KinesisStatsMetric.VEHICLE_CURRENT_GEAR_INT_VALUE,
        future=[
            KinesisStatsMetric.J1939_ETC5_TRANSMISSION_REVERSE_DIRECTION_SWITCH_STATE_INT_VALUE,
            KinesisStatsMetric.J1939_TCO1_DIRECTION_INDICATOR_STATE_INT_VALUE,
        ],
        vg_release="VG-37",
    ),
    door_status=SignalConfig(
        current=KinesisStatsMetric.DOOR_OPEN_STATUS_INT_VALUE,
        future=[],
    ),
    can_connected=SignalConfig(
        current=KinesisStatsMetric.OSD_CAN_CONNECTED_INT_VALUE,
        future=[],
    ),
)


# Reusable Column definitions for device-level coverage struct fields
TURN_SIGNAL_DETECTED_COLUMN = Column(
    name="turn_signal_detected",
    type=DataType.BOOLEAN,
    nullable=True,
    metadata=Metadata(comment="TRUE if turn signal detected"),
)
REVERSE_GEAR_DETECTED_COLUMN = Column(
    name="reverse_gear_detected",
    type=DataType.BOOLEAN,
    nullable=True,
    metadata=Metadata(comment="TRUE if reverse gear detected"),
)
DOOR_STATUS_DETECTED_COLUMN = Column(
    name="door_status_detected",
    type=DataType.BOOLEAN,
    nullable=True,
    metadata=Metadata(comment="TRUE if door status detected"),
)
CAN_CONNECTED_DETECTED_COLUMN = Column(
    name="can_connected_detected",
    type=DataType.BOOLEAN,
    nullable=True,
    metadata=Metadata(comment="TRUE if CAN connected"),
)
TURN_SIGNAL_RECOMMENDATION_COLUMN = Column(
    name="turn_signal_recommendation",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Turn signal install recommendation"),
)
REVERSE_GEAR_RECOMMENDATION_COLUMN = Column(
    name="reverse_gear_recommendation",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Reverse gear install recommendation"),
)
DOOR_STATUS_RECOMMENDATION_COLUMN = Column(
    name="door_status_recommendation",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Door status install recommendation"),
)
AIM4_RECOMMENDATION_COLUMN = Column(
    name="aim4_recommendation",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(
        comment="AIM4 consolidated recommendation (worst case of turn signal + reverse gear)"
    ),
)
RECOMMENDATION_EXPLANATION_COLUMN = Column(
    name="recommendation_explanation",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(
        comment="Human-readable explanation of the combined turn signal and reverse gear recommendations"
    ),
)
VG_IMPROVEMENT_REASON_COLUMN = Column(
    name="vg_improvement_reason",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(
        comment="Explanation of potential firmware upgrade benefits. Compares pre-VG36 coverage with VG-36/VG-37 to identify if turn signal or reverse gear support would improve with firmware updates."
    ),
)

# Device-level coverage struct type (reused for each firmware version)
# Note: recommendation_explanation and vg_improvement_reason are top-level columns
DEVICE_COVERAGE_STRUCT = struct_from_columns(
    TURN_SIGNAL_DETECTED_COLUMN,
    REVERSE_GEAR_DETECTED_COLUMN,
    DOOR_STATUS_DETECTED_COLUMN,
    CAN_CONNECTED_DETECTED_COLUMN,
    TURN_SIGNAL_RECOMMENDATION_COLUMN,
    REVERSE_GEAR_RECOMMENDATION_COLUMN,
    DOOR_STATUS_RECOMMENDATION_COLUMN,
    AIM4_RECOMMENDATION_COLUMN,
)

# Schema definition using firmware version structs
COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    # Firmware version coverage structs
    Column(
        name="pre_vg36_fw",
        type=DEVICE_COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="Pre-VG36 baseline: turn_signal=j1939_oel_turn_signal_switch_state, reverse_gear=vehicle_current_gear"
        ),
    ),
    Column(
        name="vg36_fw",
        type=DEVICE_COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="VG-36.1.0 (Dec 18, 2025): adds SPN 2367/2369 (left/right turn signal) for octo trigger logic"
        ),
    ),
    Column(
        name="vg37_fw",
        type=DEVICE_COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="VG-37 target: adds SPN 767 (reverse direction) and SPN 1619 (direction indicator) for reverse gear"
        ),
    ),
    # Top-level explanation columns (based on pre_vg36_fw baseline)
    VG_IMPROVEMENT_REASON_COLUMN,
    RECOMMENDATION_EXPLANATION_COLUMN,
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = r"""
WITH 
-- Get device dimensions (vehicle config + device config)
device_dimensions AS (
    SELECT
        date,
        org_id,
        device_id,
        market,
        make,
        model,
        year,
        engine_model,
        engine_type,
        fuel_type,
        device_type,
        product_name,
        variant_name,
        cable_name
    FROM product_analytics_staging.dim_telematics_coverage_full
    WHERE date BETWEEN '{date_start}' AND '{date_end}'
        AND device_type = 'VG'
        AND make != 'UNKNOWN' 
        AND model != 'UNKNOWN' 
        AND year != -1
),

-- Get coverage metrics for all candidate signals (binary: signal present or not)
coverage_data AS (
    SELECT
        date,
        org_id,
        device_id,
        type,
        TRUE as is_covered
    FROM product_analytics_staging.fct_telematics_coverage_rollup_full
    WHERE date BETWEEN '{date_start}' AND '{date_end}'
        AND type IN (
            -- Current signals
            '{TURN_SIGNAL_LABEL}', '{REVERSE_GEAR_LABEL}', '{DOOR_STATUS_LABEL}', '{CAN_CONNECTED_LABEL}',
            -- Future turn signal alternatives (VG-36+)
            '{LEFT_TURN_SIGNAL_LABEL}', '{RIGHT_TURN_SIGNAL_LABEL}',
            -- Future reverse gear alternatives (VG-37+)
            '{REVERSE_DIRECTION_LABEL}', '{DIRECTION_INDICATOR_LABEL}'
        )
        AND count_month_days_covered > 0
),

-- Pivot coverage data by signal type
pivoted_coverage AS (
    SELECT
        date,
        org_id,
        device_id,
        -- Current signals (pre-VG36)
        COALESCE(MAX(CASE WHEN type = '{TURN_SIGNAL_LABEL}' THEN is_covered END), FALSE) as turn_signal_current,
        COALESCE(MAX(CASE WHEN type = '{REVERSE_GEAR_LABEL}' THEN is_covered END), FALSE) as reverse_gear_current,
        COALESCE(MAX(CASE WHEN type = '{DOOR_STATUS_LABEL}' THEN is_covered END), FALSE) as door_status,
        COALESCE(MAX(CASE WHEN type = '{CAN_CONNECTED_LABEL}' THEN is_covered END), FALSE) as can_connected,
        -- Future turn signal alternatives (VG-36+)
        COALESCE(MAX(CASE WHEN type = '{LEFT_TURN_SIGNAL_LABEL}' THEN is_covered END), FALSE) as left_turn_signal,
        COALESCE(MAX(CASE WHEN type = '{RIGHT_TURN_SIGNAL_LABEL}' THEN is_covered END), FALSE) as right_turn_signal,
        -- Future reverse gear alternatives (VG-37+)
        COALESCE(MAX(CASE WHEN type = '{REVERSE_DIRECTION_LABEL}' THEN is_covered END), FALSE) as reverse_direction,
        COALESCE(MAX(CASE WHEN type = '{DIRECTION_INDICATOR_LABEL}' THEN is_covered END), FALSE) as direction_indicator
    FROM coverage_data
    GROUP BY date, org_id, device_id
),

-- Join dimensions with coverage and compute per-FW-version detection flags
joined AS (
    SELECT
        d.date,
        d.org_id,
        d.device_id,
        -- Door status and CAN connected are the same across all FW versions
        COALESCE(c.door_status, FALSE) as door_status,
        COALESCE(c.can_connected, FALSE) as can_connected,
        
        -- Current FW (pre-VG36): original signals only
        COALESCE(c.turn_signal_current, FALSE) as turn_signal_current,
        COALESCE(c.reverse_gear_current, FALSE) as reverse_gear_current,
        
        -- VG-36 FW: improved turn signal (current OR left/right), reverse gear unchanged
        COALESCE(c.turn_signal_current, FALSE) 
            OR COALESCE(c.left_turn_signal, FALSE) 
            OR COALESCE(c.right_turn_signal, FALSE) as turn_signal_vg36,
        COALESCE(c.reverse_gear_current, FALSE) as reverse_gear_vg36,
        
        -- VG-37 FW (best case): VG-36 turn signal + improved reverse gear
        COALESCE(c.turn_signal_current, FALSE) 
            OR COALESCE(c.left_turn_signal, FALSE) 
            OR COALESCE(c.right_turn_signal, FALSE) as turn_signal_vg37,
        COALESCE(c.reverse_gear_current, FALSE) 
            OR COALESCE(c.reverse_direction, FALSE) 
            OR COALESCE(c.direction_indicator, FALSE) as reverse_gear_vg37
    FROM device_dimensions d
    LEFT JOIN pivoted_coverage c
        ON d.date = c.date
        AND d.org_id = c.org_id
        AND d.device_id = c.device_id
)

SELECT
    CAST(date AS STRING) AS date,
    org_id,
    device_id,
    
    -- Current FW struct (pre-VG36)
    STRUCT(
        turn_signal_current as turn_signal_detected,
        reverse_gear_current as reverse_gear_detected,
        door_status as door_status_detected,
        can_connected as can_connected_detected,
        CASE WHEN turn_signal_current THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as turn_signal_recommendation,
        CASE WHEN reverse_gear_current THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as reverse_gear_recommendation,
        CASE WHEN door_status THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as door_status_recommendation,
        CASE WHEN turn_signal_current AND reverse_gear_current THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as aim4_recommendation
    ) as pre_vg36_fw,
    
    -- VG-36 FW struct (improved turn signal)
    STRUCT(
        turn_signal_vg36 as turn_signal_detected,
        reverse_gear_vg36 as reverse_gear_detected,
        door_status as door_status_detected,
        can_connected as can_connected_detected,
        CASE WHEN turn_signal_vg36 THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as turn_signal_recommendation,
        CASE WHEN reverse_gear_vg36 THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as reverse_gear_recommendation,
        CASE WHEN door_status THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as door_status_recommendation,
        CASE WHEN turn_signal_vg36 AND reverse_gear_vg36 THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as aim4_recommendation
    ) as vg36_fw,
    
    -- VG-37 FW struct (best case: improved turn signal + reverse gear)
    STRUCT(
        turn_signal_vg37 as turn_signal_detected,
        reverse_gear_vg37 as reverse_gear_detected,
        door_status as door_status_detected,
        can_connected as can_connected_detected,
        CASE WHEN turn_signal_vg37 THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as turn_signal_recommendation,
        CASE WHEN reverse_gear_vg37 THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as reverse_gear_recommendation,
        CASE WHEN door_status THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as door_status_recommendation,
        CASE WHEN turn_signal_vg37 AND reverse_gear_vg37 THEN '{REC_PLUG_AND_PLAY}' ELSE '{REC_HARD_WIRED_REQUIRED}' END as aim4_recommendation
    ) as vg37_fw,
    
    -- VG improvement reason: explain what firmware upgrades would improve
    CASE
        -- Both improve with firmware
        WHEN NOT turn_signal_current AND turn_signal_vg36 AND NOT reverse_gear_current AND reverse_gear_vg37
        THEN 'VG-36 adds turn signal support. VG-37 adds reverse gear support.'
        -- Only turn signal improves (VG-36)
        WHEN NOT turn_signal_current AND turn_signal_vg36
        THEN 'VG-36 adds turn signal support via SPN 2367/2369.'
        -- Only reverse gear improves (VG-37)
        WHEN NOT reverse_gear_current AND reverse_gear_vg37
        THEN 'VG-37 adds reverse gear support via SPN 767/1619.'
        -- No improvements available
        ELSE NULL
    END as vg_improvement_reason,
    
    -- Top-level recommendation explanation (based on pre_vg36_fw baseline)
    CASE
        WHEN turn_signal_current AND reverse_gear_current THEN 'Both features fully support Plug & Play.'
        WHEN turn_signal_current AND NOT reverse_gear_current THEN 'Reverse gear requires hard-wiring.'
        WHEN NOT turn_signal_current AND reverse_gear_current THEN 'Turn signal requires hard-wiring.'
        ELSE 'Both turn signal and reverse gear require hard-wiring.'
    END as recommendation_explanation

FROM joined
ORDER BY date, org_id, device_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Per-device installation recommendations for VG devices organized by firmware version. Each firmware struct (pre_vg36_fw, vg36_fw, vg37_fw) contains signal detection flags and recommendations showing what's available at that release level. vg37_fw represents the best-case future scenario.",
        row_meaning="Each row represents installation recommendations for one VG device on one date (org_id, device_id, date), with structs for each firmware version.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP_FULL),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_INSTALL_RECOMMENDATIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_install_recommendations(context: AssetExecutionContext) -> str:
    """
    Generate per-device installation recommendations based on signal coverage by firmware version.

    This table provides device-level granularity for AIM4 feature recommendations,
    organized by firmware version to show the progression of signal support:

    - pre_vg36_fw: Pre-VG36 baseline using original signals
    - vg36_fw: VG-36+ with improved turn signal support (left/right turn signals)
    - vg37_fw: VG-37+ best case with improved turn signal + reverse gear support

    Each struct contains:
    - Detection flags (turn_signal, reverse_gear, door_status, can_connected)
    - Per-signal recommendations
    - Consolidated AIM4 recommendation (worst case of turn signal + reverse gear)

    Use cases:
    - Compare coverage across firmware versions
    - Identify devices that will benefit from firmware upgrades
    - Customer-specific compatibility checks by firmware
    - Feed for aggregate recommendation summaries
    """

    return format_date_partition_query(
        query=QUERY,
        context=context,
        # Current signals (pre-VG36)
        TURN_SIGNAL_LABEL=SIGNAL_CONFIGS.turn_signal.current_label,
        REVERSE_GEAR_LABEL=SIGNAL_CONFIGS.reverse_gear.current_label,
        DOOR_STATUS_LABEL=SIGNAL_CONFIGS.door_status.current_label,
        CAN_CONNECTED_LABEL=SIGNAL_CONFIGS.can_connected.current_label,
        # Future turn signal alternatives (VG-36+)
        LEFT_TURN_SIGNAL_LABEL=SIGNAL_CONFIGS.turn_signal.future[0].label,
        RIGHT_TURN_SIGNAL_LABEL=SIGNAL_CONFIGS.turn_signal.future[1].label,
        # Future reverse gear alternatives (VG-37+)
        REVERSE_DIRECTION_LABEL=SIGNAL_CONFIGS.reverse_gear.future[0].label,
        DIRECTION_INDICATOR_LABEL=SIGNAL_CONFIGS.reverse_gear.future[1].label,
        # Recommendation labels
        REC_PLUG_AND_PLAY=InstallRecommendation.PLUG_AND_PLAY.label,
        REC_HARD_WIRED_REQUIRED=InstallRecommendation.HARD_WIRED_REQUIRED.label,
    )
