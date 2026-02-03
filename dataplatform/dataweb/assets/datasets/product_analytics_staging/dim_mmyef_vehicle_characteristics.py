"""
dim_mmyef_vehicle_characteristics

Lookup table for MMYEF ID to vehicle characteristics mapping.
Provides a compact reference table with unique vehicle characteristic combinations 
and their corresponding MMYEF hash for efficient joins with aggregate tables.
This eliminates the need to join the large dim_device_vehicle_properties table 
when only vehicle characteristics are needed for analysis.
"""

from dataclasses import replace
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    DataType,
    Metadata,
    ColumnType,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

# Import standardized MMYEF definitions from dim_device_vehicle_properties
from .dim_device_vehicle_properties import MMYEF_ID

COLUMNS = [
    ColumnType.DATE,
    replace(
        MMYEF_ID, nullable=False, primary_key=True
    ),  # Make non-nullable and primary key for this lookup table
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
    ColumnType.POWERTRAIN,
    ColumnType.FUEL_GROUP,
    ColumnType.TRIM,
    Column(
        name="device_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of distinct devices with this vehicle characteristic combination on this date"
        ),
    ),
    Column(
        name="org_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of distinct organizations with this vehicle characteristic combination on this date"
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
SELECT
    date,
    mmyef_id,
    TRIM(make) AS make,
    TRIM(model) AS model,
    year,
    TRIM(engine_model) AS engine_model,
    powertrain,
    fuel_group,
    TRIM(trim) AS trim,
    COUNT(DISTINCT device_id) AS device_count,
    COUNT(DISTINCT org_id) AS org_count
FROM product_analytics_staging.dim_device_vehicle_properties
WHERE mmyef_id IS NOT NULL
  AND date BETWEEN "{date_start}" AND "{date_end}"
GROUP BY ALL
ORDER BY date, mmyef_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Date-partitioned lookup table for MMYEF ID to vehicle characteristics mapping with fleet prevalence metrics. "
        "Provides daily snapshots of unique vehicle characteristic combinations with their corresponding "
        "MMYEF hash, device counts, and org counts for efficient joins with aggregate tables. Tracks the evolution of vehicle types "
        "in the fleet over time, their prevalence across devices and organizations, and enables time-based analysis while maintaining "
        "compact size compared to the full device-level dim_device_vehicle_properties table. Optimized for fast lookups and "
        "joins with MMYEF-based aggregate tables.",
        row_meaning="Each row represents a unique combination of vehicle characteristics "
        "(make, model, year, engine, fuel, powertrain, trim) observed on a specific date with its MMYEF hash ID, "
        "along with the count of distinct devices and organizations having this vehicle configuration.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_MMYEF_VEHICLE_CHARACTERISTICS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def dim_mmyef_vehicle_characteristics(context: AssetExecutionContext) -> str:
    """
    Create date-partitioned lookup table mapping MMYEF IDs to vehicle characteristics with prevalence metrics.

    This asset:
    1. Aggregates vehicle characteristic combinations from dim_device_vehicle_properties per date
    2. Creates a date-partitioned dimension table with (date, mmyef_id) as primary keys
    3. Includes all vehicle characteristics (make, model, year, engine, fuel, powertrain, trim)
    4. Counts distinct devices and organizations for each vehicle configuration
    5. Tracks evolution of vehicle types and their prevalence in the fleet over time
    6. Provides efficient reference for time-aware aggregate table joins

    Benefits:
    - Much smaller than full dim_device_vehicle_properties (unique combinations per date)
    - Includes prevalence metrics (device_count, org_count) for filtering and analysis
    - Consistent partitioning structure with source table for optimal joins
    - Enables time-based analysis of vehicle population changes
    - Optimized for joins with MMYEF-based aggregate tables
    - Tracks when new vehicle types appear in the fleet and how widespread they are

    Usage:
    - Join aggregate tables on (mmyef_id, date) for vehicle characteristics
    - Analyze vehicle type population changes over time
    - Filter by prevalence (e.g., only analyze vehicle configs with > 100 devices)
    - Filter aggregates by vehicle dimensions with date context
    - Reference table for dashboards requiring historical vehicle metadata with fleet size
    """
    return format_date_partition_query(QUERY, context)
