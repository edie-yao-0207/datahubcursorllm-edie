"""
Cost hierarchy definitions for consistent aggregation grouping across cost tables.

This module provides column definitions and pre-built SQL for:
- Billing service IDs (for dim_billing_services <-> fct_table_cost_daily joins)
- Hierarchy IDs (for GROUPING SETS aggregations in agg_costs_rolling, etc.)
"""

from dataweb.userpkgs.firmware.hash_utils import (
    hash_id_sql,
    build_grouping_id_sql,
    build_grouping_label_sql,
    build_grouping_columns_sql,
)


# ============================================================================
# Billing Service ID (for dim_billing_services <-> fct_table_cost_daily)
# ============================================================================

# Columns that define a unique billing service - single source of truth
# Must include all dimensions that can vary per row in billing.dataplatform_costs
BILLING_SERVICE_COLUMNS = [
    "date",
    "service",
    "region",
    "costAllocation",
    "team",
    "productgroup",
    "dataplatformfeature",
    "dataplatformjobtype",
    "databricks_sku",
    "databricks_workspace_id",
]

# SQL expression for billing_service_id - generated from BILLING_SERVICE_COLUMNS
BILLING_SERVICE_ID_SQL = hash_id_sql(BILLING_SERVICE_COLUMNS)


# ============================================================================
# Hierarchy Dimension Columns
# ============================================================================

# Define hierarchy columns ONCE - all SQL is generated from this
# Order: least granular to most granular (for CONCAT_WS label building)
HIERARCHY_DIMENSION_COLUMNS = [
    "region",
    "team",
    "product_group",
    "dataplatform_feature",
    "service",
]


# ============================================================================
# Pre-built SQL constants (generated from HIERARCHY_DIMENSION_COLUMNS)
# ============================================================================

# Pre-built SQL for hierarchy_id in GROUPING SETS context (no alias)
HIERARCHY_ID_SQL = build_grouping_id_sql(HIERARCHY_DIMENSION_COLUMNS)

# Pre-built SQL for hierarchy_label in GROUPING SETS context (no alias)
# Shows VALUES: "us-west-2.firmwarevdp.signal_decoding"
HIERARCHY_LABEL_SQL = build_grouping_label_sql(HIERARCHY_DIMENSION_COLUMNS)

# Pre-built SQL for grouping_columns in GROUPING SETS context (no alias)
# Shows COLUMN NAMES: "region.team.service"
GROUPING_COLUMNS_SQL = build_grouping_columns_sql(HIERARCHY_DIMENSION_COLUMNS)
