"""
fct_telematics_coverage_qoq

Device-level quarter-over-quarter signal coverage tracking.
Compares coverage for each device/signal type between current date and 90 days prior.
Only includes devices that were CAN-connected >20 days on both dates (eligible cohort).
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
    ColumnType.DEVICE_ID,
    ColumnType.ORG_ID,
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
        name="has_coverage_previous_quarter",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Coverage indicator 90 days prior (true=has coverage, false=no coverage). Based on count_month_days_covered > 0."
        ),
    ),
    Column(
        name="has_coverage_current_quarter",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Coverage indicator on current date (true=has coverage, false=no coverage). Based on count_month_days_covered > 0."
        ),
    ),
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

QUERY = """
-- Load coverage data once for the entire needed range (lookback + current)
-- This covers both d1 dates (90 days before first date) and d2 dates (last date in range)
WITH coverage_data AS (
  SELECT
    f.date,
    f.device_id,
    f.org_id,
    f.type,
    f.count_month_days_covered
  FROM product_analytics_staging.fct_telematics_coverage_rollup_full f
  WHERE f.date BETWEEN DATE_SUB(DATE("{date_start}"), 90) AND DATE("{date_end}")
),

-- Identify CAN-connected devices (>20 days) for d2 dates (current dates in range)
d2_can_connected AS (
  SELECT DISTINCT
    cd.date,
    cd.device_id,
    cd.org_id
  FROM coverage_data cd
  JOIN product_analytics_staging.dim_telematics_coverage_full d USING (date, device_id, org_id)
  WHERE cd.type = 'can_connected'
    AND cd.count_month_days_covered > 20
    AND cd.date BETWEEN DATE("{date_start}") AND DATE("{date_end}")
),

-- Identify CAN-connected devices (>20 days) for d1 dates (90 days before current)
d1_can_connected AS (
  SELECT DISTINCT
    DATE_ADD(cd.date, 90) AS date,
    cd.device_id,
    cd.org_id
  FROM coverage_data cd
  JOIN product_analytics_staging.dim_telematics_coverage_full d USING (date, device_id, org_id)
  WHERE cd.type = 'can_connected'
    AND cd.count_month_days_covered > 20
    AND cd.date BETWEEN DATE_SUB(DATE("{date_start}"), 90) AND DATE_SUB(DATE("{date_end}"), 90)
),

-- For each date in range, find eligible cohort (devices active on BOTH d2 and d1)
eligible_cohorts AS (
  SELECT
    d2_devices.date,
    d2_devices.device_id,
    d2_devices.org_id
  FROM d2_can_connected d2_devices
  JOIN d1_can_connected d1_devices USING (date, device_id, org_id)
),

  -- Get actual coverage for eligible devices (no cross join!)
  -- Process both d1 and d2 periods in parallel with UNION ALL
  coverage_union AS (
    SELECT
      DATE_ADD(cd.date, 90) AS date,
      cd.device_id,
      cd.org_id,
      cd.type,
      CASE WHEN cd.count_month_days_covered > 0 THEN 1 ELSE 0 END AS has_coverage,
      'd1' AS period
    FROM coverage_data cd
    JOIN eligible_cohorts ec 
      ON ec.date = DATE_ADD(cd.date, 90)
      AND ec.device_id = cd.device_id
      AND ec.org_id = cd.org_id
    WHERE cd.date BETWEEN DATE_SUB(DATE("{date_start}"), 90) AND DATE_SUB(DATE("{date_end}"), 90)
    
    UNION ALL
    
    SELECT
      cd.date,
      cd.device_id,
      cd.org_id,
      cd.type,
      CASE WHEN cd.count_month_days_covered > 0 THEN 1 ELSE 0 END AS has_coverage,
      'd2' AS period
    FROM coverage_data cd
    JOIN eligible_cohorts ec USING (date, device_id, org_id)
    WHERE cd.date BETWEEN DATE("{date_start}") AND DATE("{date_end}")
  ),
  
  -- Aggregate to get max coverage per device/type/period
  coverage_agg AS (
    SELECT
      date,
      device_id,
      org_id,
      type,
      period,
      MAX(has_coverage) AS has_coverage
    FROM coverage_union
    GROUP BY date, device_id, org_id, type, period
  ),
  
  -- Pivot to get d1 and d2 side by side
  coverage_pivot AS (
    SELECT
      date,
      device_id,
      org_id,
      type,
      CAST(MAX(CASE WHEN period = 'd1' THEN has_coverage ELSE 0 END) AS BOOLEAN) AS coverage_d1,
      CAST(MAX(CASE WHEN period = 'd2' THEN has_coverage ELSE 0 END) AS BOOLEAN) AS coverage_d2
    FROM coverage_agg
    GROUP BY date, device_id, org_id, type
  )
  
-- Return device-level coverage comparison
SELECT
  CAST(date AS STRING) AS date,
  device_id,
  org_id,
  type,
  coverage_d1 AS has_coverage_previous_quarter,
  coverage_d2 AS has_coverage_current_quarter
FROM coverage_pivot
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Device-level quarter-over-quarter signal coverage tracking. "
        "Records signal coverage for each device on current date and 90 days prior (one quarter). "
        "Only includes devices that were CAN-connected >20 days on BOTH dates to ensure like-to-like comparison. "
        "Coverage is based on count_month_days_covered > 0 from fct_telematics_coverage_rollup_full, "
        "following the same pattern as agg_telematics_actual_coverage. "
        "Changes can be derived from the two boolean columns (gained = current true & previous false, etc.).",
        row_meaning="Each row represents one device's coverage status for one signal type, "
        "with boolean flags for coverage on current date and 90 days prior.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP_FULL),
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_QOQ.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_coverage_qoq(context: AssetExecutionContext) -> str:
    """
    Device-level quarter-over-quarter signal coverage comparison.

    This daily partitioned fact table provides device-level coverage data for QoQ analysis.
    Each row represents one device's coverage status for one signal type, comparing
    current date to 90 days prior.

    Key logic:
    1. Loads coverage data for extended range (date_start - 90 to date_end)
    2. For each date, identifies eligible cohort: devices CAN-connected >20 days on BOTH:
       - Current quarter (current date)
       - Previous quarter (90 days prior)
    3. For each eligible device/type combination, records:
       - has_coverage_previous_quarter: Coverage 90 days ago (true/false based on count_month_days_covered > 0)
       - has_coverage_current_quarter: Coverage on current date (true/false based on count_month_days_covered > 0)

    The like-to-like comparison ensures fair analysis by only including devices
    that were actively connected on both dates, avoiding bias from device churn.

    Downstream analysis can easily compute coverage changes:
    - Gained: has_coverage_current_quarter = true AND has_coverage_previous_quarter = false
    - Lost: has_coverage_current_quarter = false AND has_coverage_previous_quarter = true
    - Retained: both true
    - No coverage: both false

    This device-level table enables flexible downstream aggregations by various dimensions
    (signal type, vehicle attributes, org, etc.) without re-computing the cohort logic.

    Coverage follows the same definition as agg_telematics_actual_coverage:
    count_month_days_covered > 0 indicates the device has coverage for that signal type.
    """
    return format_date_partition_query(QUERY, context)
