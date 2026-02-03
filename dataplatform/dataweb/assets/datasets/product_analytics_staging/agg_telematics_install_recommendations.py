"""
Installation recommendations by vehicle configuration based on population coverage

Applies recommendation thresholds to population-level signal coverage from 
agg_telematics_actual_coverage_normalized. Identifies most frequent install configurations
and provides confidence levels for sales and installation planning.

Each firmware version struct contains the expected coverage at that release level:
- pre_vg36_fw: Pre-VG36 baseline (j1939_oel_turn_signal_switch_state, vehicle_current_gear)
- vg36_fw: VG-36.1.0 (rollout completes Dec 18, 2025) - adds SPN 2367 (left turn signal) and
  SPN 2369 (right turn signal) support for octo trigger logic input
- vg37_fw: VG-37 target - adds SPN 767 (reverse direction) and SPN 1619 (direction indicator)
  for improved reverse gear detection
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
from dataweb.assets.datasets.product_analytics_staging.fct_telematics_install_recommendations import (
    InstallRecommendation,
    SIGNAL_CONFIGS,
    SignalConfig,
    SignalConfigs,
    # Reuse recommendation columns from fct table
    TURN_SIGNAL_RECOMMENDATION_COLUMN,
    REVERSE_GEAR_RECOMMENDATION_COLUMN,
    DOOR_STATUS_RECOMMENDATION_COLUMN,
    AIM4_RECOMMENDATION_COLUMN,
)


# Coverage columns specific to population-level (ratios instead of booleans)
TURN_SIGNAL_COVERAGE_COLUMN = Column(
    name="turn_signal_coverage",
    type=DataType.DOUBLE,
    nullable=True,
    metadata=Metadata(comment="Turn signal coverage ratio (0.0-1.0)"),
)
REVERSE_GEAR_COVERAGE_COLUMN = Column(
    name="reverse_gear_coverage",
    type=DataType.DOUBLE,
    nullable=True,
    metadata=Metadata(comment="Reverse gear coverage ratio (0.0-1.0)"),
)
DOOR_STATUS_COVERAGE_COLUMN = Column(
    name="door_status_coverage",
    type=DataType.DOUBLE,
    nullable=True,
    metadata=Metadata(comment="Door status coverage ratio (0.0-1.0)"),
)
CAN_CONNECTED_COVERAGE_COLUMN = Column(
    name="can_connected_coverage",
    type=DataType.DOUBLE,
    nullable=True,
    metadata=Metadata(comment="CAN connected coverage ratio (0.0-1.0)"),
)
# Recommendation columns are imported from fct_telematics_install_recommendations

# Population-level coverage struct type (reused for each firmware version)
POPULATION_COVERAGE_STRUCT = struct_from_columns(
    TURN_SIGNAL_COVERAGE_COLUMN,
    REVERSE_GEAR_COVERAGE_COLUMN,
    DOOR_STATUS_COVERAGE_COLUMN,
    CAN_CONNECTED_COVERAGE_COLUMN,
    TURN_SIGNAL_RECOMMENDATION_COLUMN,
    REVERSE_GEAR_RECOMMENDATION_COLUMN,
    DOOR_STATUS_RECOMMENDATION_COLUMN,
    AIM4_RECOMMENDATION_COLUMN,
)


# Schema definition
COLUMNS = [
    ColumnType.DATE,
    ColumnType.GROUPING_HASH,
    # Population metrics
    Column(
        name="count_distinct_device_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Number of devices in this configuration"),
    ),
    Column(
        name="count_distinct_org_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of distinct organizations with this configuration"
        ),
    ),
    Column(
        name="most_frequent_install",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="True if this is the most common install config for this vehicle (by CAN connected devices)"
        ),
    ),
    Column(
        name="recommendation_confidence",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Confidence level: Low (<50 devices OR 1 org <200 devices), Medium (50-200 devices multiple orgs OR 200+ devices 1 org), High (200+ devices multiple orgs)"
        ),
    ),
    # Firmware version coverage structs
    Column(
        name="pre_vg36_fw",
        type=POPULATION_COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="Pre-VG36 baseline: turn_signal=j1939_oel_turn_signal_switch_state, reverse_gear=vehicle_current_gear"
        ),
    ),
    Column(
        name="vg36_fw",
        type=POPULATION_COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="VG-36.1.0 (Dec 18, 2025): adds SPN 2367/2369 (left/right turn signal) for octo trigger logic"
        ),
    ),
    Column(
        name="vg37_fw",
        type=POPULATION_COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="VG-37 target: adds SPN 767 (reverse direction) and SPN 1619 (direction indicator) for reverse gear"
        ),
    ),
    # VG improvement explanation
    Column(
        name="vg_improvement_reason",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Explanation of potential firmware upgrade benefits. Compares pre-VG36 coverage with VG-36/VG-37 to identify if turn signal or reverse gear support would improve with firmware updates."
        ),
    ),
    # Recommendation explanation based on consolidation matrix
    Column(
        name="recommendation_explanation",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Human-readable explanation of the AIM4 consolidated recommendation based on the combination of turn signal and reverse gear recommendations."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = r"""
WITH 
-- Pivot signal coverage from normalized coverage table (all candidate signals)
coverage_pivoted AS (
    SELECT
        date,
        grouping_hash,
        -- Current signals (pre-VG36)
        MAX(CASE WHEN type = '{TURN_SIGNAL_LABEL}' THEN percent_coverage END) as turn_signal_current,
        MAX(CASE WHEN type = '{REVERSE_GEAR_LABEL}' THEN percent_coverage END) as reverse_gear_current,
        MAX(CASE WHEN type = '{DOOR_STATUS_LABEL}' THEN percent_coverage END) as door_status,
        MAX(CASE WHEN type = '{CAN_CONNECTED_LABEL}' THEN percent_coverage END) as can_connected,
        -- Future turn signal alternatives (VG-36+)
        MAX(CASE WHEN type = '{LEFT_TURN_SIGNAL_LABEL}' THEN percent_coverage END) as left_turn_signal,
        MAX(CASE WHEN type = '{RIGHT_TURN_SIGNAL_LABEL}' THEN percent_coverage END) as right_turn_signal,
        -- Future reverse gear alternatives (VG-37+)
        MAX(CASE WHEN type = '{REVERSE_DIRECTION_LABEL}' THEN percent_coverage END) as reverse_direction,
        MAX(CASE WHEN type = '{DIRECTION_INDICATOR_LABEL}' THEN percent_coverage END) as direction_indicator
    FROM {product_analytics_staging}.agg_telematics_actual_coverage_normalized
    WHERE date BETWEEN '{date_start}' AND '{date_end}'
        AND type IN (
            -- Current signals
            '{TURN_SIGNAL_LABEL}', '{REVERSE_GEAR_LABEL}', '{DOOR_STATUS_LABEL}', '{CAN_CONNECTED_LABEL}',
            -- Future turn signal alternatives (VG-36+)
            '{LEFT_TURN_SIGNAL_LABEL}', '{RIGHT_TURN_SIGNAL_LABEL}',
            -- Future reverse gear alternatives (VG-37+)
            '{REVERSE_DIRECTION_LABEL}', '{DIRECTION_INDICATOR_LABEL}'
        )
        -- Note: grouping_level and device_type filters are applied via populations table join
    GROUP BY date, grouping_hash
),

-- Compute per-FW-version coverage using GREATEST for combined signals
coverage_by_fw AS (
    SELECT
        date,
        grouping_hash,
        -- Door status and CAN connected are the same across all FW versions
        door_status,
        can_connected,
        
        -- Current FW (pre-VG36): original signals only
        turn_signal_current,
        reverse_gear_current,
        
        -- VG-36 FW: improved turn signal (GREATEST of current + left/right), reverse gear unchanged
        GREATEST(
            COALESCE(turn_signal_current, 0),
            COALESCE(left_turn_signal, 0),
            COALESCE(right_turn_signal, 0)
        ) as turn_signal_vg36,
        reverse_gear_current as reverse_gear_vg36,
        
        -- VG-37 FW (best case): VG-36 turn signal + improved reverse gear
        GREATEST(
            COALESCE(turn_signal_current, 0),
            COALESCE(left_turn_signal, 0),
            COALESCE(right_turn_signal, 0)
        ) as turn_signal_vg37,
        GREATEST(
            COALESCE(reverse_gear_current, 0),
            COALESCE(reverse_direction, 0),
            COALESCE(direction_indicator, 0)
        ) as reverse_gear_vg37
    FROM coverage_pivoted
),

-- Join populations with coverage (keep dimensions for ranking logic)
joined AS (
    SELECT
        pop.date,
        pop.grouping_hash,
        pop.market,
        pop.make,
        pop.model,
        pop.year,
        pop.engine_model,
        pop.engine_type,
        pop.fuel_type,
        pop.device_type,
        pop.product_name,
        pop.variant_name,
        pop.cable_name,
        pop.count_distinct_device_id,
        pop.count_distinct_org_id,
        -- Confidence based on population size (not signal-specific)
        -- Low: (<50 devices) OR (1 org AND <200 devices)
        -- High: (200+ devices AND multiple orgs)
        -- Medium: (50-200 devices AND multiple orgs) OR (200+ devices AND 1 org)
        CASE
            WHEN pop.count_distinct_device_id < 50 THEN 'Low'
            WHEN pop.count_distinct_org_id = 1 AND pop.count_distinct_device_id < 200 THEN 'Low'
            WHEN pop.count_distinct_device_id >= 200 AND pop.count_distinct_org_id > 1 THEN 'High'
            WHEN pop.count_distinct_device_id BETWEEN 50 AND 199 AND pop.count_distinct_org_id > 1 THEN 'Medium'
            WHEN pop.count_distinct_device_id >= 200 AND pop.count_distinct_org_id = 1 THEN 'Medium'
            ELSE 'Low'
        END as recommendation_confidence,
        -- Coverage by FW version
        c.door_status,
        c.can_connected,
        c.turn_signal_current,
        c.reverse_gear_current,
        c.turn_signal_vg36,
        c.reverse_gear_vg36,
        c.turn_signal_vg37,
        c.reverse_gear_vg37
    FROM product_analytics_staging.agg_telematics_populations pop
    LEFT JOIN coverage_by_fw c
        ON pop.date = c.date
        AND pop.grouping_hash = c.grouping_hash
    WHERE pop.date BETWEEN '{date_start}' AND '{date_end}'
        AND pop.grouping_level = 'market + device_type + product_name + variant_name + cable_name + engine_type + fuel_type + make + model + year + engine_model'
        AND pop.device_type = 'VG'
        AND pop.make != 'UNKNOWN'
        AND pop.model != 'UNKNOWN'
        AND pop.year != -1
),

-- Rank install configurations by device count to find most frequent install per vehicle
install_ranks AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date, make, model, year, engine_model, engine_type, fuel_type, market
            ORDER BY count_distinct_device_id DESC, variant_name, cable_name
        ) as install_rank
    FROM joined
)

SELECT
    CAST(date AS STRING) AS date,
    grouping_hash,
    count_distinct_device_id,
    count_distinct_org_id,
    CASE WHEN install_rank = 1 THEN TRUE ELSE FALSE END as most_frequent_install,
    recommendation_confidence,
    
    -- Current FW struct (pre-VG36)
    STRUCT(
        turn_signal_current as turn_signal_coverage,
        reverse_gear_current as reverse_gear_coverage,
        door_status as door_status_coverage,
        can_connected as can_connected_coverage,
        CASE
            WHEN COALESCE(turn_signal_current, 0) >= {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_PLUG_AND_PLAY}'
            WHEN COALESCE(turn_signal_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            WHEN COALESCE(turn_signal_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            ELSE '{REC_HARD_WIRED_REQUIRED}'
        END as turn_signal_recommendation,
        CASE
            WHEN COALESCE(reverse_gear_current, 0) >= {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_PLUG_AND_PLAY}'
            WHEN COALESCE(reverse_gear_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            WHEN COALESCE(reverse_gear_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            ELSE '{REC_HARD_WIRED_REQUIRED}'
        END as reverse_gear_recommendation,
        CASE
            WHEN COALESCE(door_status, 0) >= {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_PLUG_AND_PLAY}'
            WHEN COALESCE(door_status, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            WHEN COALESCE(door_status, 0) >= {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            ELSE '{REC_HARD_WIRED_REQUIRED}'
        END as door_status_recommendation,
        CASE
            WHEN COALESCE(turn_signal_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD} OR COALESCE(reverse_gear_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_HARD_WIRED_REQUIRED}'
            WHEN COALESCE(turn_signal_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD} OR COALESCE(reverse_gear_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            WHEN COALESCE(turn_signal_current, 0) < {PLUG_AND_PLAY_THRESHOLD} OR COALESCE(reverse_gear_current, 0) < {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            ELSE '{REC_PLUG_AND_PLAY}'
        END as aim4_recommendation
    ) as pre_vg36_fw,
    
    -- VG-36 FW struct (improved turn signal)
    STRUCT(
        turn_signal_vg36 as turn_signal_coverage,
        reverse_gear_vg36 as reverse_gear_coverage,
        door_status as door_status_coverage,
        can_connected as can_connected_coverage,
        CASE
            WHEN COALESCE(turn_signal_vg36, 0) >= {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_PLUG_AND_PLAY}'
            WHEN COALESCE(turn_signal_vg36, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            WHEN COALESCE(turn_signal_vg36, 0) >= {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            ELSE '{REC_HARD_WIRED_REQUIRED}'
        END as turn_signal_recommendation,
        CASE
            WHEN COALESCE(reverse_gear_vg36, 0) >= {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_PLUG_AND_PLAY}'
            WHEN COALESCE(reverse_gear_vg36, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            WHEN COALESCE(reverse_gear_vg36, 0) >= {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            ELSE '{REC_HARD_WIRED_REQUIRED}'
        END as reverse_gear_recommendation,
        CASE
            WHEN COALESCE(door_status, 0) >= {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_PLUG_AND_PLAY}'
            WHEN COALESCE(door_status, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            WHEN COALESCE(door_status, 0) >= {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            ELSE '{REC_HARD_WIRED_REQUIRED}'
        END as door_status_recommendation,
        CASE
            WHEN COALESCE(turn_signal_vg36, 0) < {LIKELY_HARD_WIRED_THRESHOLD} OR COALESCE(reverse_gear_vg36, 0) < {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_HARD_WIRED_REQUIRED}'
            WHEN COALESCE(turn_signal_vg36, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD} OR COALESCE(reverse_gear_vg36, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            WHEN COALESCE(turn_signal_vg36, 0) < {PLUG_AND_PLAY_THRESHOLD} OR COALESCE(reverse_gear_vg36, 0) < {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            ELSE '{REC_PLUG_AND_PLAY}'
        END as aim4_recommendation
    ) as vg36_fw,
    
    -- VG-37 FW struct (best case: improved turn signal + reverse gear)
    STRUCT(
        turn_signal_vg37 as turn_signal_coverage,
        reverse_gear_vg37 as reverse_gear_coverage,
        door_status as door_status_coverage,
        can_connected as can_connected_coverage,
        CASE
            WHEN COALESCE(turn_signal_vg37, 0) >= {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_PLUG_AND_PLAY}'
            WHEN COALESCE(turn_signal_vg37, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            WHEN COALESCE(turn_signal_vg37, 0) >= {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            ELSE '{REC_HARD_WIRED_REQUIRED}'
        END as turn_signal_recommendation,
        CASE
            WHEN COALESCE(reverse_gear_vg37, 0) >= {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_PLUG_AND_PLAY}'
            WHEN COALESCE(reverse_gear_vg37, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            WHEN COALESCE(reverse_gear_vg37, 0) >= {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            ELSE '{REC_HARD_WIRED_REQUIRED}'
        END as reverse_gear_recommendation,
        CASE
            WHEN COALESCE(door_status, 0) >= {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_PLUG_AND_PLAY}'
            WHEN COALESCE(door_status, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            WHEN COALESCE(door_status, 0) >= {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            ELSE '{REC_HARD_WIRED_REQUIRED}'
        END as door_status_recommendation,
        CASE
            WHEN COALESCE(turn_signal_vg37, 0) < {LIKELY_HARD_WIRED_THRESHOLD} OR COALESCE(reverse_gear_vg37, 0) < {LIKELY_HARD_WIRED_THRESHOLD} THEN '{REC_HARD_WIRED_REQUIRED}'
            WHEN COALESCE(turn_signal_vg37, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD} OR COALESCE(reverse_gear_vg37, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_HARD_WIRED}'
            WHEN COALESCE(turn_signal_vg37, 0) < {PLUG_AND_PLAY_THRESHOLD} OR COALESCE(reverse_gear_vg37, 0) < {PLUG_AND_PLAY_THRESHOLD} THEN '{REC_LIKELY_PLUG_AND_PLAY}'
            ELSE '{REC_PLUG_AND_PLAY}'
        END as aim4_recommendation
    ) as vg37_fw,
    
    -- VG improvement reason: explain firmware upgrade benefits
    -- Note: Turn signal improvement (VG-36) omitted as rollout completes Dec 18, 2025
    CASE
        -- Reverse gear would improve with VG-37
        WHEN COALESCE(reverse_gear_vg37, 0) > COALESCE(reverse_gear_current, 0)
        THEN 'Improved gear status support expected in {REVERSE_GEAR_VG_RELEASE}. For urgent support needs, please contact VDP team.'
        -- No improvements available
        ELSE NULL
    END as vg_improvement_reason,
    
    -- Recommendation explanation based on consolidation matrix (14 combinations)
    -- Uses pre_vg36_fw recommendations as the baseline for explanations
    CASE
        -- Both Hard-Wired Required
        WHEN COALESCE(turn_signal_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD} 
             AND COALESCE(reverse_gear_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD}
        THEN 'Both turn signal and reverse gear require hard-wiring.'
        
        -- Turn signal Hard-Wired Required + Reverse gear Plug & Play
        WHEN COALESCE(turn_signal_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) >= {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Turn signal requires hard-wiring.'
        
        -- Turn signal Hard-Wired Required + Reverse gear Likely Hard-Wired
        WHEN COALESCE(turn_signal_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD}
        THEN 'Turn signal requires hard-wiring. Reverse gear likely needs hard-wiring. Please install VG to confirm availability.'
        
        -- Turn signal Hard-Wired Required + Reverse gear Likely Plug & Play
        WHEN COALESCE(turn_signal_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) < {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Turn signal requires hard-wiring. Reverse gear may have minor coverage gaps. Please install VG to confirm availability.'
        
        -- Reverse gear Hard-Wired Required + Turn signal Plug & Play
        WHEN COALESCE(reverse_gear_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) >= {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Reverse gear requires hard-wiring.'
        
        -- Reverse gear Hard-Wired Required + Turn signal Likely Hard-Wired
        WHEN COALESCE(reverse_gear_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD}
        THEN 'Reverse gear requires hard-wiring. Turn signal likely needs hard-wiring. Please install VG to confirm availability.'
        
        -- Reverse gear Hard-Wired Required + Turn signal Likely Plug & Play
        WHEN COALESCE(reverse_gear_current, 0) < {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) < {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Reverse gear requires hard-wiring. Turn signal may have minor coverage gaps. Please install VG to confirm availability.'
        
        -- Both Likely Hard-Wired
        WHEN COALESCE(turn_signal_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD}
        THEN 'Both features likely need hard-wiring. Please install VG to confirm availability.'
        
        -- Turn signal Likely Hard-Wired + Reverse gear Plug & Play
        WHEN COALESCE(turn_signal_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) >= {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Turn signal likely needs hard-wiring. Please install VG to confirm availability.'
        
        -- Reverse gear Likely Hard-Wired + Turn signal Plug & Play
        WHEN COALESCE(reverse_gear_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) >= {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Reverse gear likely needs hard-wiring. Please install VG to confirm availability.'
        
        -- Turn signal Likely Hard-Wired + Reverse gear Likely Plug & Play
        WHEN COALESCE(turn_signal_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) < {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Turn signal likely needs hard-wiring. Reverse gear may have minor coverage gaps. Please install VG to confirm availability.'
        
        -- Reverse gear Likely Hard-Wired + Turn signal Likely Plug & Play
        WHEN COALESCE(reverse_gear_current, 0) >= {LIKELY_HARD_WIRED_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) < {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) < {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Reverse gear likely needs hard-wiring. Turn signal may have minor coverage gaps. Please install VG to confirm availability.'
        
        -- Both Likely Plug & Play
        WHEN COALESCE(turn_signal_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) < {PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) < {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Both features may have minor gaps. Please install VG to confirm availability.'
        
        -- Turn signal Likely Plug & Play + Reverse gear Plug & Play
        WHEN COALESCE(turn_signal_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) < {PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) >= {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Turn signal may have minor coverage gaps. Please install VG to confirm availability.'
        
        -- Reverse gear Likely Plug & Play + Turn signal Plug & Play
        WHEN COALESCE(reverse_gear_current, 0) >= {LIKELY_PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) < {PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(turn_signal_current, 0) >= {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Reverse gear may have minor coverage gaps. Please install VG to confirm availability.'
        
        -- Both Plug & Play
        WHEN COALESCE(turn_signal_current, 0) >= {PLUG_AND_PLAY_THRESHOLD}
             AND COALESCE(reverse_gear_current, 0) >= {PLUG_AND_PLAY_THRESHOLD}
        THEN 'Both features fully support Plug & Play.'
        
        ELSE NULL
    END as recommendation_explanation

FROM install_ranks
ORDER BY count_distinct_device_id DESC
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Installation recommendations by vehicle configuration organized by firmware version. Each firmware struct (pre_vg36_fw, vg36_fw, vg37_fw) contains coverage ratios and recommendations showing what's expected at that release level. vg37_fw represents the best-case future scenario.",
        row_meaning="Each row represents installation recommendations for one vehicle configuration (grouping_hash) with structs for each firmware version.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_POPULATIONS),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_ACTUAL_COVERAGE_NORMALIZED),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_INSTALL_RECOMMENDATIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_install_recommendations(context: AssetExecutionContext) -> str:
    """
    Generate installation recommendations at vehicle configuration level organized by firmware version.

    This table provides installation guidance for sales and install teams by applying
    recommendation logic to population-level coverage metrics, organized by firmware version
    to show the progression of signal support:

    - pre_vg36_fw: Pre-VG36 baseline using original signals
    - vg36_fw: VG-36+ with improved turn signal support (left/right turn signals)
    - vg37_fw: VG-37+ best case with improved turn signal + reverse gear support

    Each struct contains:
    - Coverage ratios (turn_signal, reverse_gear, door_status, can_connected)
    - Per-signal recommendations (Plug & Play / Likely Plug & Play / Likely Hard-Wired / Hard-Wired Required)
    - Consolidated AIM4 recommendation (worst case of turn signal + reverse gear)

    Use cases:
    - Compare expected coverage across firmware versions
    - Pre-sale vehicle compatibility assessment by firmware
    - Install planning showing improvement potential with FW upgrades
    - Customer expectations management with future support outlook
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
        # Thresholds and labels from InstallRecommendation enum
        PLUG_AND_PLAY_THRESHOLD=InstallRecommendation.PLUG_AND_PLAY.threshold,
        LIKELY_PLUG_AND_PLAY_THRESHOLD=InstallRecommendation.LIKELY_PLUG_AND_PLAY.threshold,
        LIKELY_HARD_WIRED_THRESHOLD=InstallRecommendation.LIKELY_HARD_WIRED.threshold,
        REC_PLUG_AND_PLAY=InstallRecommendation.PLUG_AND_PLAY.label,
        REC_LIKELY_PLUG_AND_PLAY=InstallRecommendation.LIKELY_PLUG_AND_PLAY.label,
        REC_LIKELY_HARD_WIRED=InstallRecommendation.LIKELY_HARD_WIRED.label,
        REC_HARD_WIRED_REQUIRED=InstallRecommendation.HARD_WIRED_REQUIRED.label,
        # VG release version for improvement message (turn signal omitted - VG-36 rollout complete Dec 18, 2025)
        REVERSE_GEAR_VG_RELEASE=SIGNAL_CONFIGS.reverse_gear.vg_release,
    )
