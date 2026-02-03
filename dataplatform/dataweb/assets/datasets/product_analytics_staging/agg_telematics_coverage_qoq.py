"""
agg_telematics_coverage_qoq

Aggregated quarter-over-quarter signal coverage summary by signal type.
Aggregates device-level QoQ coverage data from fct_telematics_coverage_qoq.
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
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description

COLUMNS = [
    ColumnType.DATE,
    Column(
        name="type",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Signal type from fct_telematics_coverage_rollup_full"
        ),
    ),
    Column(
        name="eligible_devices",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total distinct devices in eligible cohort (CAN-connected >20 days on both dates)"
        ),
    ),
    # Current quarter coverage counts
    Column(
        name="count_ice_current",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="ICE devices with coverage on current quarter"),
    ),
    Column(
        name="count_hydrogen_current",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Hydrogen devices with coverage on current quarter"),
    ),
    Column(
        name="count_hybrid_current",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Hybrid devices with coverage on current quarter"),
    ),
    Column(
        name="count_bev_current",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="BEV devices with coverage on current quarter"),
    ),
    Column(
        name="count_phev_current",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="PHEV devices with coverage on current quarter"),
    ),
    Column(
        name="count_unknown_current",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Unknown engine type devices with coverage on current quarter"
        ),
    ),
    # Change metrics (gained and lost)
    Column(
        name="count_ice_gained",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="ICE devices that gained coverage (current true, previous false)"
        ),
    ),
    Column(
        name="count_hydrogen_gained",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Hydrogen devices that gained coverage"),
    ),
    Column(
        name="count_hybrid_gained",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Hybrid devices that gained coverage"),
    ),
    Column(
        name="count_bev_gained",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="BEV devices that gained coverage"),
    ),
    Column(
        name="count_phev_gained",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="PHEV devices that gained coverage"),
    ),
    Column(
        name="count_unknown_gained",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Unknown engine type devices that gained coverage"),
    ),
    Column(
        name="count_ice_lost",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="ICE devices that lost coverage (current false, previous true)"
        ),
    ),
    Column(
        name="count_hydrogen_lost",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Hydrogen devices that lost coverage"),
    ),
    Column(
        name="count_hybrid_lost",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Hybrid devices that lost coverage"),
    ),
    Column(
        name="count_bev_lost",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="BEV devices that lost coverage"),
    ),
    Column(
        name="count_phev_lost",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="PHEV devices that lost coverage"),
    ),
    Column(
        name="count_unknown_lost",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Unknown engine type devices that lost coverage"),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
WITH coverage_qoq AS (
  SELECT
    qoq.*,
    COALESCE(dim.engine_type, 'UNKNOWN') AS engine_type
  FROM {product_analytics_staging}.fct_telematics_coverage_qoq qoq
  LEFT JOIN product_analytics_staging.dim_telematics_coverage_full dim USING (date, device_id, org_id)
  WHERE qoq.date BETWEEN "{date_start}" AND "{date_end}"
)

SELECT
  date,
  type,
  COUNT(DISTINCT device_id) AS eligible_devices,
  -- Current quarter coverage
  COUNT_IF(has_coverage_current_quarter AND engine_type = 'ICE') AS count_ice_current,
  COUNT_IF(has_coverage_current_quarter AND engine_type = 'HYDROGEN') AS count_hydrogen_current,
  COUNT_IF(has_coverage_current_quarter AND engine_type = 'HYBRID') AS count_hybrid_current,
  COUNT_IF(has_coverage_current_quarter AND engine_type = 'BEV') AS count_bev_current,
  COUNT_IF(has_coverage_current_quarter AND engine_type = 'PHEV') AS count_phev_current,
  COUNT_IF(has_coverage_current_quarter AND engine_type = 'UNKNOWN') AS count_unknown_current,
  -- Gained coverage (current true, previous false)
  COUNT_IF(has_coverage_current_quarter AND NOT has_coverage_previous_quarter AND engine_type = 'ICE') AS count_ice_gained,
  COUNT_IF(has_coverage_current_quarter AND NOT has_coverage_previous_quarter AND engine_type = 'HYDROGEN') AS count_hydrogen_gained,
  COUNT_IF(has_coverage_current_quarter AND NOT has_coverage_previous_quarter AND engine_type = 'HYBRID') AS count_hybrid_gained,
  COUNT_IF(has_coverage_current_quarter AND NOT has_coverage_previous_quarter AND engine_type = 'BEV') AS count_bev_gained,
  COUNT_IF(has_coverage_current_quarter AND NOT has_coverage_previous_quarter AND engine_type = 'PHEV') AS count_phev_gained,
  COUNT_IF(has_coverage_current_quarter AND NOT has_coverage_previous_quarter AND engine_type = 'UNKNOWN') AS count_unknown_gained,
  -- Lost coverage (current false, previous true)
  COUNT_IF(NOT has_coverage_current_quarter AND has_coverage_previous_quarter AND engine_type = 'ICE') AS count_ice_lost,
  COUNT_IF(NOT has_coverage_current_quarter AND has_coverage_previous_quarter AND engine_type = 'HYDROGEN') AS count_hydrogen_lost,
  COUNT_IF(NOT has_coverage_current_quarter AND has_coverage_previous_quarter AND engine_type = 'HYBRID') AS count_hybrid_lost,
  COUNT_IF(NOT has_coverage_current_quarter AND has_coverage_previous_quarter AND engine_type = 'BEV') AS count_bev_lost,
  COUNT_IF(NOT has_coverage_current_quarter AND has_coverage_previous_quarter AND engine_type = 'PHEV') AS count_phev_lost,
  COUNT_IF(NOT has_coverage_current_quarter AND has_coverage_previous_quarter AND engine_type = 'UNKNOWN') AS count_unknown_lost
FROM coverage_qoq
GROUP BY date, type
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Comprehensive quarter-over-quarter signal coverage aggregation by signal type and engine type. "
        "Aggregates device-level QoQ coverage data with three key metrics per engine type: "
        "current quarter coverage, previous quarter coverage, and change metrics (gained/lost). "
        "Only includes devices that were CAN-connected >20 days on BOTH dates to ensure fair comparison. "
        "Provides complete QoQ analysis without requiring self-joins or date arithmetic. "
        "Follows engine type breakdown pattern similar to agg_telematics_actual_coverage.",
        row_meaning="Each row represents complete QoQ coverage analysis for one signal type on one date, "
        "with current, previous, gained, and lost counts broken down by engine type (ICE, HYDROGEN, HYBRID, BEV, PHEV, UNKNOWN).",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_QOQ),
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_COVERAGE_QOQ.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_coverage_qoq(context: AssetExecutionContext) -> str:
    """
    Comprehensive quarter-over-quarter coverage aggregation with full change tracking.

    This aggregation provides complete QoQ analysis by computing four metrics per engine type:
    1. Current quarter coverage counts (count_*_current)
    2. Previous quarter coverage counts (count_*_previous)
    3. Devices that gained coverage (count_*_gained)
    4. Devices that lost coverage (count_*_lost)

    Each signal type shows QoQ trends across all engine types:
    ICE, HYDROGEN, HYBRID, BEV, PHEV, UNKNOWN

    Example use case:
    - count_ice_current: 1000 ICE devices have coverage today
    - count_ice_previous: 950 ICE devices had coverage 90 days ago
    - count_ice_gained: 100 ICE devices gained coverage
    - count_ice_lost: 50 ICE devices lost coverage
    - Net change: +50 (gained - lost)

    Benefits:
    - No self-joins required to calculate QoQ changes
    - All change metrics pre-computed and ready for dashboards
    - Can easily calculate rates: gained/eligible, lost/previous, etc.
    - Downstream normalized table can apply metadata filters

    The aggregation is fast since the complex cohort logic is already
    handled in fct_telematics_coverage_qoq. This can be further normalized using
    fct_telematics_stat_metadata to filter by applicable engine types per signal type.
    """
    return format_date_partition_query(QUERY, context)
