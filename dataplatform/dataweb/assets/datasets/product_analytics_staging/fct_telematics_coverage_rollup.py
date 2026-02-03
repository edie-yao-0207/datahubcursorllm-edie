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
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """
WITH devices AS (
  SELECT
    dim.date,
    dim.org_id,
    dim.device_id,
    ff.has_aggregated_engine_hours
  FROM product_analytics_staging.dim_telematics_coverage_full AS dim
  LEFT JOIN product_analytics_staging.fct_telematics_ff_impacted_devices AS ff USING (date, org_id, device_id)
  WHERE dim.date BETWEEN DATE_SUB("{date_start}", 27) AND "{date_end}"
),

coverage AS (
  SELECT
    devices.date,
    devices.org_id,
    devices.device_id,
    coverage.type,
    coverage.is_covered AS day_covered,
    1 AS priority
  FROM devices
  JOIN product_analytics.agg_device_stats_secondary_coverage AS coverage
  USING (date, org_id, device_id)
),

overrides AS (
  SELECT
    date,
    org_id,
    device_id,
    'engine_seconds' AS type,
    has_aggregated_engine_hours AS day_covered,
    2 AS priority
  FROM devices 
  WHERE has_aggregated_engine_hours
),

combined_coverage AS (
  SELECT * FROM coverage
  UNION ALL
  SELECT * FROM overrides
),

ranked_coverage AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY date, type, org_id, device_id
      ORDER BY priority DESC
    ) AS row_num
  FROM combined_coverage
),

resolved_coverage AS (
  SELECT
    date,
    org_id,
    device_id,
    type,
    day_covered
  FROM ranked_coverage
  WHERE row_num = 1
),

windowed_coverage AS (
  SELECT
    date,
    type,
    org_id,
    device_id,
    day_covered,

    COUNT_IF(day_covered) OVER (
      PARTITION BY org_id, device_id, type
      ORDER BY CAST(date AS TIMESTAMP)
      RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
    ) AS count_week_days_covered,
    count_week_days_covered > 0 AS week_covered,
    CAST(count_week_days_covered / 7.0 AS DOUBLE) AS percent_week_days_covered,

    COUNT_IF(day_covered) OVER (
      PARTITION BY org_id, device_id, type
      ORDER BY CAST(date AS TIMESTAMP)
      RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND CURRENT ROW
    ) AS count_month_days_covered,
    count_month_days_covered > 0 AS month_covered,
    CAST(count_month_days_covered / 28.0 AS DOUBLE) AS percent_month_days_covered
  FROM resolved_coverage
)

SELECT *
FROM windowed_coverage
WHERE date BETWEEN "{date_start}" AND "{date_end}"
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.TYPE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="day_covered",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the device was covered on the day."),
    ),
    Column(
        name="count_week_days_covered",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The number of days the device was supported in the last 7 days."),
    ),
    Column(
        name="week_covered",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the device was covered in the last 7 days."),
    ),
    Column(
        name="percent_week_days_covered",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="The percentage of days the device was supported in the last 7 days."),
    ),
    Column(
        name="count_month_days_covered",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The number of days the device was supported in the last 28 days."),
    ),
    Column(
        name="month_covered",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Whether the device was covered in the last 28 days."),
    ),
    Column(
        name="percent_month_days_covered",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="The percentage of days the device was supported in the last 28 days."),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

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
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_FF_IMPACTED_DEVICES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_coverage_rollup(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
