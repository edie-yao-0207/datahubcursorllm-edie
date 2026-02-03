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
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.assets.datasets.product_analytics_staging.fct_telematics_coverage_rollup import (
    PRIMARY_KEYS,
    NON_NULL_COLUMNS,
    SCHEMA,
)

QUERY = """
WITH base_coverage AS (
  SELECT *
  FROM product_analytics_staging.fct_telematics_coverage_rollup
  WHERE date BETWEEN DATE_SUB(DATE("{date_start}"), 27) AND DATE("{date_end}")
),

-- All (entity, type) combinations ever seen
active_entities AS (
  SELECT DISTINCT org_id, device_id, type
  FROM base_coverage
),

-- Instead of exploding dates across all active entities,
-- only join entities to dates that actually had a possible match
entity_date_candidates AS (
  SELECT
    entity.org_id,
    entity.device_id,
    entity.type,
    calendar_date AS date
  FROM active_entities entity
  CROSS JOIN (
    SELECT sequence(DATE("{date_start}"), DATE("{date_end}")) AS dates
  ) d
  LATERAL VIEW EXPLODE(d.dates) AS calendar_date
),

-- Join to available coverage rows within 28-day window
joined AS (
  SELECT
    CAST(edc.date AS STRING) AS date,
    edc.org_id,
    edc.device_id,
    edc.type,
    bc.date AS source_date,
    bc.day_covered,
    bc.count_week_days_covered,
    bc.week_covered,
    bc.percent_week_days_covered,
    bc.count_month_days_covered,
    bc.month_covered,
    bc.percent_month_days_covered
  FROM entity_date_candidates edc
  JOIN base_coverage bc
    ON bc.org_id = edc.org_id
    AND bc.device_id = edc.device_id
    AND bc.type = edc.type
    AND bc.date BETWEEN DATE_SUB(edc.date, 27) AND edc.date
),

-- Rank coverage rows so we only pick the most recent per (entity, date)
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY org_id, device_id, type, date
      ORDER BY source_date DESC
    ) AS rn
  FROM joined
)

-- Final output: most recent match for each device/type/date
SELECT
  date,
  org_id,
  device_id,
  type,
  day_covered,
  count_week_days_covered,
  week_covered,
  percent_week_days_covered,
  count_month_days_covered,
  month_covered,
  percent_month_days_covered
FROM ranked
WHERE rn = 1
"""

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Statistics on telematics coverage.",
        row_meaning="Statistics on telematics coverage for a device over a given time period.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP_FULL.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_coverage_rollup_full(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
