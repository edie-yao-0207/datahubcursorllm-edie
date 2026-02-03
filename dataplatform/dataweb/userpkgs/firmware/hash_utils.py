"""
Generic hash ID and GROUPING SETS utilities for SQL generation.

Uses xxhash64 for fast, deterministic hashing that returns BIGINT.

NOTE: Domain-specific hash constants (TABLE_ID_SQL, HIERARCHY_ID_SQL, etc.)
should be defined in their respective dimension tables, not here.
This module provides the generic building blocks and shared column definitions.
"""

from typing import List, Optional


# ============================================================================
# Basic Hash Utilities
# ============================================================================


def hash_id_sql(columns: List[str], coalesce_empty: bool = True) -> str:
    """
    Generate SQL expression for a consistent hash ID from a list of columns.

    Uses xxhash64 for fast, non-cryptographic hashing that returns BIGINT.
    The hash is deterministic - same inputs always produce same hash.

    Args:
        columns: List of column names to include in the hash
        coalesce_empty: If True, COALESCE nulls to empty string (default).
                        Set to False if columns are guaranteed non-null.

    Returns:
        SQL expression that produces a BIGINT hash

    Example:
        >>> hash_id_sql(['date', 'service', 'region'])
        "xxhash64(CONCAT_WS('|', COALESCE(CAST(date AS STRING), ''), ...))"
    """
    if coalesce_empty:
        parts = ", ".join([f"COALESCE(CAST({col} AS STRING), '')" for col in columns])
    else:
        parts = ", ".join([f"CAST({col} AS STRING)" for col in columns])
    return f"xxhash64(CONCAT_WS('|', {parts}))"


def hash_id_sql_raw(raw_columns: List[str]) -> str:
    """
    Generate SQL expression for a hash ID using raw column expressions.

    Use this when you need to apply transformations to columns before hashing,
    or when columns have different names in source vs target.

    Args:
        raw_columns: List of raw SQL expressions (e.g., "COALESCE(costAllocation, '')")

    Returns:
        SQL expression that produces a BIGINT hash

    Example:
        >>> hash_id_sql_raw(["CAST(date AS STRING)", "COALESCE(service, '')", "COALESCE(region, '')"])
        "xxhash64(CONCAT_WS('|', CAST(date AS STRING), COALESCE(service, ''), COALESCE(region, '')))"
    """
    parts = ", ".join(raw_columns)
    return f"xxhash64(CONCAT_WS('|', {parts}))"


# ============================================================================
# GROUPING SETS Utilities
# ============================================================================


def build_grouping_id_sql(columns: List[str]) -> str:
    """
    Generate a unique hash ID for GROUPING SETS context.

    Includes GROUPING indicators to distinguish "column is NULL" from
    "column not in grouping set".

    Args:
        columns: List of dimension column names used in GROUPING SETS

    Returns:
        SQL expression that produces a unique BIGINT hash per grouping
    """
    # Include GROUPING indicators first (to distinguish grouping levels)
    grouping_parts = [f"CAST(GROUPING({col}) AS STRING)" for col in columns]
    # Then include actual values (when column is in grouping)
    value_parts = [
        f"CASE WHEN GROUPING({col}) = 0 THEN COALESCE({col}, '') ELSE '' END"
        for col in columns
    ]
    all_parts = grouping_parts + value_parts
    return f"xxhash64(CONCAT_WS('|', {', '.join(all_parts)}))"


def build_grouping_label_sql(columns: List[str]) -> str:
    """
    Generate a human-readable label for GROUPING SETS context.

    Creates a dot-separated label of VALUES like "us-west-2.firmwarevdp.dataweb".

    Args:
        columns: List of dimension column names (least to most granular)

    Returns:
        SQL CASE expression that returns a readable grouping label
    """
    # Check if all columns are grouped out (overall level)
    all_grouped_out = " AND ".join([f"GROUPING({col}) = 1" for col in columns])
    # Build dot-separated label from included columns
    concat_parts = [f"CASE WHEN GROUPING({col}) = 0 THEN {col} END" for col in columns]

    return f"""CASE
  WHEN {all_grouped_out} THEN 'overall'
  ELSE CONCAT_WS('.', {', '.join(concat_parts)})
END"""


def build_grouping_columns_sql(
    columns: List[str], aliases: Optional[List[str]] = None
) -> str:
    """
    Generate a dot-separated list of COLUMN NAMES in the grouping.

    Creates a string like "region.team.service" showing which dimensions
    are included (not grouped out) in each GROUPING SETS row.

    Use this to easily select populations by which dimensions they include:
      WHERE grouping_columns = 'team'                    -- team-only (no region)
      WHERE grouping_columns = 'region.team'             -- region + team
      WHERE grouping_columns LIKE '%service%'            -- any grouping with service

    Args:
        columns: List of dimension column names (least to most granular)
        aliases: Optional list of human-readable aliases for the columns.
                 If provided, these are used in the output instead of raw column names.
                 Must be same length as columns.

    Returns:
        SQL expression that returns dot-separated column names (or aliases)
    """
    labels = aliases if aliases else columns
    if aliases and len(aliases) != len(columns):
        raise ValueError(
            f"aliases length ({len(aliases)}) must match columns length ({len(columns)})"
        )

    # Check if all columns are grouped out (overall level)
    all_grouped_out = " AND ".join([f"GROUPING({col}) = 1" for col in columns])
    # Build dot-separated list of column NAMES (or aliases) that are included
    concat_parts = [
        f"CASE WHEN GROUPING({col}) = 0 THEN '{label}' END"
        for col, label in zip(columns, labels)
    ]

    return f"""CASE
  WHEN {all_grouped_out} THEN 'overall'
  ELSE CONCAT_WS('.', {', '.join(concat_parts)})
END"""


# ============================================================================
# Shared Hierarchy Column Definitions
# ============================================================================

# Import schema types here to avoid circular imports in consuming modules
from dataweb.userpkgs.firmware.schema import Column, DataType, Metadata

# These columns are used by ALL hierarchy dimension tables:
# - dim_cost_hierarchies
# - dim_dashboard_hierarchies
# - dim_user_hierarchies

HIERARCHY_ID_COLUMN = Column(
    name="hierarchy_id",
    type=DataType.LONG,
    nullable=False,
    primary_key=True,
    metadata=Metadata(
        comment="xxhash64 of grouping level + dimension values - FK to hierarchy dimension table"
    ),
)

GROUPING_COLUMNS_COLUMN = Column(
    name="grouping_columns",
    type=DataType.STRING,
    nullable=False,
    primary_key=True,
    metadata=Metadata(
        comment="Dot-separated COLUMN NAMES in this grouping (e.g., 'region.team.service'). Use to select populations."
    ),
)

HIERARCHY_LABEL_COLUMN = Column(
    name="hierarchy_label",
    type=DataType.STRING,
    nullable=False,
    metadata=Metadata(
        comment="Dot-separated VALUES identifying this grouping (e.g., 'us-west-2.firmwarevdp')"
    ),
)
