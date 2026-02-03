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
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.assets.datasets.product_analytics_staging.fct_telematics_coverage_dimensions import (
    PRIMARY_KEYS,
    NON_NULL_COLUMNS,
    SCHEMA,
)

QUERY = """
WITH base_output AS (
  SELECT *
  FROM product_analytics_staging.fct_telematics_coverage_dimensions
  WHERE date BETWEEN DATE_SUB(DATE('{date_start}'), 27) AND DATE('{date_end}')
),

calendar AS (
  SELECT sequence(DATE('{date_start}'), DATE('{date_end}')) AS dates
),

dates AS (
  SELECT EXPLODE(dates) AS snapshot_date FROM calendar
),

device_dates AS (
  SELECT
    d.snapshot_date,
    f.device_id,
    f.org_id,
    f.date AS record_date,
    f.*
  FROM base_output f
  JOIN dates d
    ON f.date BETWEEN DATE_SUB(d.snapshot_date, 27) AND d.snapshot_date
)

, ranked_latest AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY snapshot_date, device_id, org_id
      ORDER BY record_date DESC
    ) AS rn
  FROM device_dates
)

SELECT
  CAST(snapshot_date AS STRING) AS date,
  device_id,
  org_id,
  attr_user_note,
  attr_make,
  attr_model,
  attr_year,
  vin_make,
  vin_model,
  vin_year,
  attr_equipment_type,
  serial_pin,
  attr_user_note_vin,
  attr_user_note_model,
  prefiltered_year,
  prefiltered_model,
  make,
  model,
  year,
  equipment_type,
  device_type,
  market,
  product_name,
  variant_name,
  cable_name,
  engine_type,
  fuel_type,
  engine_model,
  internal_type,
  diagnostics_capable,
  license_plate
FROM ranked_latest
WHERE rn = 1
"""

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Device telematics coverage dimensions.",
        row_meaning="The telematics coverage dimensions of devices filled with the most recent record for each date. Each row represents the most recent device availability over the last 28 days.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_DIMENSIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_telematics_coverage_full(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
    )