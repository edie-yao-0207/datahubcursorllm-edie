"""
Signal promotion hierarchy definitions for consistent aggregation grouping.

This module provides column definitions and pre-built SQL for:
- Hierarchy IDs (for GROUPING SETS aggregations in agg_signal_promotion_daily_metrics)
- Population detail columns from signalpromotiondb.populations

Uses the generic hierarchy utilities from hash_utils.py.
"""

from dataweb.userpkgs.firmware.hash_utils import (
    build_grouping_id_sql,
    build_grouping_label_sql,
    build_grouping_columns_sql,
)
from dataweb.userpkgs.firmware.schema import Column, DataType, Metadata
from dataclasses import replace
from dataweb.userpkgs.firmware.hash_utils import HIERARCHY_ID_COLUMN


# ============================================================================
# Hierarchy Dimension Columns
# ============================================================================

# Define hierarchy columns ONCE - all SQL is generated from this
# Order: least granular to most granular (for CONCAT_WS label building)
# Note: These are the SQL column references used in GROUPING SETS
HIERARCHY_DIMENSION_COLUMNS = [
    "f.created_by",        # user_id
    "f.data_source",       # data_source
    "f.current.stage",     # current_stage
    "f.promotion_outcome", # promotion_outcome
    "f.population_uuid",   # population
]

# Column name aliases for grouping_columns output
HIERARCHY_DIMENSION_ALIASES = [
    "user",
    "data_source",
    "stage",
    "outcome",
    "population",
]


# ============================================================================
# Pre-built SQL constants (generated from HIERARCHY_DIMENSION_COLUMNS)
# ============================================================================

# Pre-built SQL for hierarchy_id in GROUPING SETS context (no alias)
HIERARCHY_ID_SQL = build_grouping_id_sql(HIERARCHY_DIMENSION_COLUMNS)

# Pre-built SQL for hierarchy_label in GROUPING SETS context (no alias)
# Shows VALUES: "12345.4.3.SUCCESS.abc-123"
HIERARCHY_LABEL_SQL = build_grouping_label_sql(HIERARCHY_DIMENSION_COLUMNS)

# Pre-built SQL for grouping_columns in GROUPING SETS context
# Shows COLUMN NAMES: "user.data_source.stage" (using aliases instead of full column paths)
GROUPING_COLUMNS_SQL = build_grouping_columns_sql(
    HIERARCHY_DIMENSION_COLUMNS, HIERARCHY_DIMENSION_ALIASES
)


# ============================================================================
# Signal Promotion Hierarchy ID Column (for aggregate tables)
# ============================================================================

SIGNAL_PROMOTION_HIERARCHY_ID_COLUMN = replace(
    HIERARCHY_ID_COLUMN,
    name="hierarchy_id",
    metadata=Metadata(
        comment="xxhash64 of grouping level + dimension values. "
        "Unique per date + dimension combination."
    ),
)


# ============================================================================
# Population Detail Columns (from signalpromotiondb.populations)
# ============================================================================

POPULATION_UUID_COLUMN = Column(
    name="population_uuid",
    type=DataType.STRING,
    nullable=True,
    primary_key=True,
    metadata=Metadata(
        comment="Population UUID. Join to signalpromotiondb.populations for details."
    ),
)

POPULATION_MAKE_COLUMN = Column(
    name="population_make",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Vehicle make (e.g., Ford, Toyota). From populations table."),
)

POPULATION_MODEL_COLUMN = Column(
    name="population_model",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Vehicle model (e.g., F-150, Camry). From populations table."),
)

POPULATION_YEAR_COLUMN = Column(
    name="population_year",
    type=DataType.INTEGER,
    nullable=True,
    metadata=Metadata(comment="Vehicle model year. From populations table."),
)

POPULATION_ENGINE_MODEL_COLUMN = Column(
    name="population_engine_model",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Engine model identifier. From populations table."),
)

POPULATION_POWERTRAIN_COLUMN = Column(
    name="population_powertrain",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Powertrain type (e.g., ICE, HEV, BEV). From populations table."),
)

POPULATION_FUEL_GROUP_COLUMN = Column(
    name="population_fuel_group",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Fuel group (e.g., gasoline, diesel). From populations table."),
)

POPULATION_TRIM_COLUMN = Column(
    name="population_trim",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Vehicle trim level. From populations table."),
)

POPULATION_TYPE_COLUMN = Column(
    name="population_type",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(
        comment="Population type name. Join to definitions.population_type for lookup."
    ),
)

# All population detail columns for easy import
POPULATION_DETAIL_COLUMNS = [
    POPULATION_MAKE_COLUMN,
    POPULATION_MODEL_COLUMN,
    POPULATION_YEAR_COLUMN,
    POPULATION_ENGINE_MODEL_COLUMN,
    POPULATION_POWERTRAIN_COLUMN,
    POPULATION_FUEL_GROUP_COLUMN,
    POPULATION_TRIM_COLUMN,
    POPULATION_TYPE_COLUMN,
]

