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
    array_of,
    struct_of,
    columns_to_schema,
    Metadata,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, ProductAnalytics, Definitions
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """
WITH unsupported AS (
  SELECT
    pop.date,
    pop.grouping_hash,
    pop.device_type,
    pop.engine_model,
    pop.engine_type,
    pop.equipment_type,
    pop.fuel_type,
    pop.make,
    pop.market,
    pop.model,
    pop.year,
    pop.engine_model,
    cov.type,
    map.obd_value AS signal
  FROM {product_analytics_staging}.agg_telematics_populations pop
  JOIN {product_analytics_staging}.agg_telematics_actual_coverage_normalized cov
    ON pop.date = cov.date AND pop.grouping_hash = cov.grouping_hash
  JOIN definitions.telematics_obd_value_mapping map
    ON cov.type = map.type
  WHERE pop.date BETWEEN "{date_start}" AND "{date_end}"
    AND pop.grouping_level = "market + device_type + engine_type + fuel_type + make + model + year + engine_model"
    AND cov.percent_coverage < 0.8
    AND pop.device_type = "VG"
),

promotions_matched AS (
  SELECT
    u.*,
    COLLECT_SET(NAMED_STRUCT('promotion_year', p.year, 'source', p.source)) AS raw_promotions
  FROM unsupported u
  JOIN product_analytics.agg_mmyef_promotions p
    ON u.date = p.date
    AND u.signal = p.signal
    AND u.make = p.make
    AND u.model = p.model
    AND ABS(p.year - u.year) <= 5
    AND (
      u.fuel_type = p.fuel_type
      OR p.fuel_type IS NULL
      OR u.fuel_type IS NULL
    )
  GROUP BY ALL
),

with_filter_and_flag AS (
  SELECT *,
    ARRAY_SORT(raw_promotions, (left, right) -> CASE
      WHEN left.promotion_year < right.promotion_year THEN -1
      WHEN left.promotion_year > right.promotion_year THEN 1
      ELSE 0
    END) AS valid_promotions,
    ARRAY_CONTAINS(TRANSFORM(raw_promotions, p -> p.promotion_year), year) AS has_existing_promotion_for_year
  FROM promotions_matched
)

SELECT
  date,
  grouping_hash,
  signal,
  valid_promotions
FROM with_filter_and_flag
WHERE NOT has_existing_promotion_for_year
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.GROUPING_HASH,
    replace(ColumnType.SIGNAL.value, primary_key=True),
    Column(
        name="valid_promotions",
        type=array_of(struct_of(
            ("promotion_year", DataType.LONG),
            ("source", DataType.STRING),
        )),
        nullable=True,
        metadata=Metadata(
            comment="The promotions that matched the signal.",
        ),
    )
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics possible signal extensions.",
        row_meaning="The possible signal extensions for telematics devices.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(Definitions.TELEMATICS_OBD_VALUE_MAPPING),
        AnyUpstream(ProductAnalytics.AGG_MMYEF_PROMOTIONS),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_POPULATIONS),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_ACTUAL_COVERAGE_NORMALIZED),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_MMYEF_PROMOTION_GAPS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_mmyef_promotion_gaps(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
