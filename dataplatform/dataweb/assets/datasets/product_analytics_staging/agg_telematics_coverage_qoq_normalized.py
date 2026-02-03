"""
agg_telematics_coverage_qoq_normalized

Normalized quarter-over-quarter signal coverage that filters by applicable engine types.
Applies fct_telematics_stat_metadata rules to only count engine types where the signal is applicable.
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
    replace(ColumnType.MARKET.value, primary_key=True),
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
            comment="Total eligible devices (CAN-connected >20 days on both dates)"
        ),
    ),
    Column(
        name="devices_with_coverage_current",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Devices with current quarter coverage (filtered by applicable engine types)"
        ),
    ),
    Column(
        name="devices_with_coverage_previous",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Devices with previous quarter coverage (derived from current - gained + lost, filtered by applicable engine types)"
        ),
    ),
    Column(
        name="devices_gained",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Devices that gained coverage QoQ (filtered by applicable engine types)"
        ),
    ),
    Column(
        name="devices_lost",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Devices that lost coverage QoQ (filtered by applicable engine types)"
        ),
    ),
    Column(
        name="devices_net_change",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Net change in coverage (devices_gained - devices_lost)"
        ),
    ),
    Column(
        name="coverage_rate_current",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Current quarter coverage rate (devices_with_coverage_current / eligible_devices)"
        ),
    ),
    Column(
        name="coverage_rate_previous",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Previous quarter coverage rate (devices_with_coverage_previous / eligible_devices)"
        ),
    ),
    Column(
        name="coverage_rate_change",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Change in coverage rate (coverage_rate_current - coverage_rate_previous, in percentage points)"
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
WITH unique_devices_per_market AS (
  -- Get unique devices per (date, market, engine_type), deduplicated across all signal types
  SELECT DISTINCT
    qoq.date,
    qoq.device_id,
    qoq.org_id,
    COALESCE(dim.market, 'UNKNOWN') AS market,
    COALESCE(dim.engine_type, 'UNKNOWN') AS engine_type
  FROM {product_analytics_staging}.fct_telematics_coverage_qoq qoq
  LEFT JOIN product_analytics_staging.dim_telematics_coverage_full dim USING (date, device_id, org_id)
  WHERE qoq.date BETWEEN "{date_start}" AND "{date_end}"
),

coverage_by_type AS (
  -- Get coverage status per (date, market, device, type)
  SELECT
    qoq.date,
    qoq.device_id,
    qoq.org_id,
    qoq.type,
    qoq.has_coverage_previous_quarter,
    qoq.has_coverage_current_quarter,
    COALESCE(dim.market, 'UNKNOWN') AS market,
    COALESCE(dim.engine_type, 'UNKNOWN') AS engine_type
  FROM {product_analytics_staging}.fct_telematics_coverage_qoq qoq
  LEFT JOIN product_analytics_staging.dim_telematics_coverage_full dim USING (date, device_id, org_id)
  WHERE qoq.date BETWEEN "{date_start}" AND "{date_end}"
),

metadata AS (
  SELECT *
  FROM product_analytics_staging.fct_telematics_stat_metadata
),

types_by_market AS (
  -- Get distinct (date, market, type) combinations that actually have data
  SELECT DISTINCT
    qoq.date,
    COALESCE(dim.market, 'UNKNOWN') AS market,
    qoq.type
  FROM {product_analytics_staging}.fct_telematics_coverage_qoq qoq
  LEFT JOIN product_analytics_staging.dim_telematics_coverage_full dim USING (date, device_id, org_id)
  WHERE qoq.date BETWEEN "{date_start}" AND "{date_end}"
),

denominators AS (
  -- Calculate eligible device count per (date, market, type)
  -- Uses unique devices per market, applying engine type filters per type
  SELECT
    t.date,
    t.market,
    t.type,
    COUNT(DISTINCT CASE
      WHEN (m.is_applicable_ice AND ud.engine_type = 'ICE')
        OR (m.is_applicable_hydrogen AND ud.engine_type = 'HYDROGEN')
        OR (m.is_applicable_hybrid AND ud.engine_type = 'HYBRID')
        OR (m.is_applicable_bev AND ud.engine_type = 'BEV')
        OR (m.is_applicable_phev AND ud.engine_type = 'PHEV')
        OR (m.is_applicable_unknown AND ud.engine_type = 'UNKNOWN')
      THEN ud.device_id
    END) AS eligible_devices
  FROM types_by_market t
  JOIN unique_devices_per_market ud USING (date, market)
  JOIN metadata m ON t.type = m.type
  GROUP BY t.date, t.market, t.type
),

per_type_stats AS (
  -- Calculate coverage metrics per (date, market, type)
  SELECT
    c.date,
    c.market,
    c.type,
    -- Current quarter coverage (filtered by applicable engine types)
    COUNT(DISTINCT CASE
      WHEN c.has_coverage_current_quarter
        AND (
          (m.is_applicable_ice AND c.engine_type = 'ICE')
          OR (m.is_applicable_hydrogen AND c.engine_type = 'HYDROGEN')
          OR (m.is_applicable_hybrid AND c.engine_type = 'HYBRID')
          OR (m.is_applicable_bev AND c.engine_type = 'BEV')
          OR (m.is_applicable_phev AND c.engine_type = 'PHEV')
          OR (m.is_applicable_unknown AND c.engine_type = 'UNKNOWN')
        )
      THEN c.device_id
    END) AS devices_with_coverage_current,

    -- Gained coverage (current true, previous false, filtered by applicable engine types)
    COUNT(DISTINCT CASE
      WHEN c.has_coverage_current_quarter
        AND NOT c.has_coverage_previous_quarter
        AND (
          (m.is_applicable_ice AND c.engine_type = 'ICE')
          OR (m.is_applicable_hydrogen AND c.engine_type = 'HYDROGEN')
          OR (m.is_applicable_hybrid AND c.engine_type = 'HYBRID')
          OR (m.is_applicable_bev AND c.engine_type = 'BEV')
          OR (m.is_applicable_phev AND c.engine_type = 'PHEV')
          OR (m.is_applicable_unknown AND c.engine_type = 'UNKNOWN')
        )
      THEN c.device_id
    END) AS devices_gained,

    -- Lost coverage (current false, previous true, filtered by applicable engine types)
    COUNT(DISTINCT CASE
      WHEN NOT c.has_coverage_current_quarter
        AND c.has_coverage_previous_quarter
        AND (
          (m.is_applicable_ice AND c.engine_type = 'ICE')
          OR (m.is_applicable_hydrogen AND c.engine_type = 'HYDROGEN')
          OR (m.is_applicable_hybrid AND c.engine_type = 'HYBRID')
          OR (m.is_applicable_bev AND c.engine_type = 'BEV')
          OR (m.is_applicable_phev AND c.engine_type = 'PHEV')
          OR (m.is_applicable_unknown AND c.engine_type = 'UNKNOWN')
        )
      THEN c.device_id
    END) AS devices_lost
  FROM coverage_by_type c
  JOIN metadata m USING (type)
  GROUP BY c.date, c.market, c.type
)

SELECT
  d.date,
  d.market,
  d.type,
  d.eligible_devices,
  COALESCE(p.devices_with_coverage_current, 0) AS devices_with_coverage_current,
  -- Derive previous quarter coverage: current - gained + lost
  COALESCE(p.devices_with_coverage_current, 0) - COALESCE(p.devices_gained, 0) + COALESCE(p.devices_lost, 0) AS devices_with_coverage_previous,
  COALESCE(p.devices_gained, 0) AS devices_gained,
  COALESCE(p.devices_lost, 0) AS devices_lost,
  -- Net change
  COALESCE(p.devices_gained, 0) - COALESCE(p.devices_lost, 0) AS devices_net_change,
  -- Current quarter coverage rate
  CAST(COALESCE(p.devices_with_coverage_current, 0) AS DOUBLE) / NULLIF(d.eligible_devices, 0) AS coverage_rate_current,
  -- Previous quarter coverage rate (derived)
  CAST(COALESCE(p.devices_with_coverage_current, 0) - COALESCE(p.devices_gained, 0) + COALESCE(p.devices_lost, 0) AS DOUBLE) / NULLIF(d.eligible_devices, 0) AS coverage_rate_previous,
  -- Coverage rate change (in percentage points)
  (CAST(COALESCE(p.devices_with_coverage_current, 0) AS DOUBLE) / NULLIF(d.eligible_devices, 0)) -
  (CAST(COALESCE(p.devices_with_coverage_current, 0) - COALESCE(p.devices_gained, 0) + COALESCE(p.devices_lost, 0) AS DOUBLE) / NULLIF(d.eligible_devices, 0)) AS coverage_rate_change
FROM denominators d
LEFT JOIN per_type_stats p USING (date, market, type)
WHERE d.eligible_devices > 0
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Normalized quarter-over-quarter signal coverage with comprehensive QoQ metrics. "
        "Applies fct_telematics_stat_metadata filtering to only count devices with applicable engine types. "
        "Provides complete QoQ analysis: current coverage, previous coverage (derived), gained, lost, net change, and rate changes. "
        "Dashboard-ready metrics showing both absolute counts and percentage changes. "
        "Previous quarter counts are derived (current - gained + lost) to avoid storing redundant data. "
        "Follows the same normalization pattern as agg_telematics_actual_coverage_normalized.",
        row_meaning="Each row represents normalized QoQ coverage analysis for one signal type on one date, "
        "with all key metrics (current, previous, gained, lost, rates) filtered by applicable engine types.",
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
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_STAT_METADATA),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_COVERAGE_QOQ_NORMALIZED.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_coverage_qoq_normalized(context: AssetExecutionContext) -> str:
    """
    Comprehensive QoQ coverage with metadata filtering and complete change metrics.

    This normalized view provides dashboard-ready QoQ analysis with:

    1. **Metadata Filtering**: Only counts devices with applicable engine types per signal
       - ICE-only signal → excludes BEV/PHEV devices
       - All engine types signal → includes all devices

    2. **Complete QoQ Metrics**:
       - devices_with_coverage_current: Devices with coverage today
       - devices_with_coverage_previous: Devices with coverage 90 days ago (derived)
       - devices_gained: Devices that gained coverage
       - devices_lost: Devices that lost coverage
       - devices_net_change: Net improvement (gained - lost)

    3. **Coverage Rates**:
       - coverage_rate_current: Current coverage %
       - coverage_rate_previous: Previous coverage % (derived)
       - coverage_rate_change: Percentage point change

    Example interpretation:
    - Signal XYZ on 2025-11-01:
      - eligible_devices: 10,000
      - devices_with_coverage_current: 8,500 (85%)
      - devices_with_coverage_previous: 8,000 (80%)
      - devices_gained: 700
      - devices_lost: 200
      - devices_net_change: +500
      - coverage_rate_change: +5.0 percentage points

    All previous quarter metrics are derived (current - gained + lost) to eliminate
    redundancy while maintaining complete QoQ visibility.
    """
    return format_date_partition_query(QUERY, context)
