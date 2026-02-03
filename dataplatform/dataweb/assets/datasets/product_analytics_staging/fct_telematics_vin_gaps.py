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
    array_of,
    struct_of,
)
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    Definitions,
    DatamodelCoreBronze,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """
WITH vin_classes AS (
  SELECT
    date,
    wmi,
    vds,
    model_year_code,
    dim.market,
    dim.make,
    dim.model,
    dim.year,
    dim.fuel_type,
    dim.engine_model,
    dim.engine_type,
    (
        is_invalid_make
        OR is_invalid_model
        OR is_invalid_year
    ) AS invalid_decoding,
    COUNT(*) as count
  FROM {product_analytics_staging}.fct_telematics_vin_issues
  JOIN {product_analytics_staging}.dim_telematics_coverage_full AS dim USING (date, org_id, device_id)
  WHERE date BETWEEN '{date_start}' AND '{date_end}'
    AND NOT (
      is_weak_vds
      OR is_weak_wmi
      OR is_malformed_vin
      OR is_missing_vin
    )
    AND dim.device_type = "VG"
  GROUP BY ALL
),

valid_vins AS (
  SELECT
    date,
    wmi,
    vds,
    model_year_code,
    market,
    make,
    model,
    year,
    engine_type,
    engine_model,
    fuel_type,
    count
  FROM vin_classes
  WHERE NOT invalid_decoding
),

invalid_vins AS (
  SELECT
    date,
    wmi,
    vds,
    model_year_code,
    market,
    make,
    model,
    year,
    engine_type,
    engine_model,
    fuel_type,
    count
  FROM vin_classes
  WHERE invalid_decoding
)

SELECT
  a.date,
  a.wmi,
  a.vds,
  a.model_year_code,
  a.market,
  a.make,
  a.model,
  a.year,
  a.engine_model,
  a.engine_type,
  a.fuel_type,
  a.count,
  FILTER(
    SORT_ARRAY(
      COLLECT_SET(
        named_struct(
          'make', b.make,
          'model', b.model,
          'year', b.year,
          'engine_model', b.engine_model,
          'engine_type', b.engine_type,
          'fuel_type', b.fuel_type
        )
      )
    ),
    x -> x.make IS NOT NULL OR x.model IS NOT NULL OR x.year IS NOT NULL
  ) AS possible_mmys
FROM invalid_vins AS a
LEFT JOIN valid_vins AS b USING (date, wmi, vds, model_year_code)
GROUP BY ALL
"""

COLUMNS = [
    ColumnType.DATE,
    replace(ColumnType.WMI.value, primary_key=True),
    replace(ColumnType.VDS.value, primary_key=True),
    replace(ColumnType.MODEL_YEAR_CODE.value, primary_key=True),
    replace(ColumnType.MARKET.value, primary_key=True),
    replace(ColumnType.MAKE.value, primary_key=True),
    replace(ColumnType.MODEL.value, primary_key=True),
    replace(ColumnType.YEAR.value, primary_key=True),
    replace(ColumnType.ENGINE_MODEL.value, primary_key=True),
    replace(ColumnType.ENGINE_TYPE.value, primary_key=True),
    replace(ColumnType.FUEL_TYPE.value, primary_key=True),
    ColumnType.COUNT,
    Column(
        name="possible_mmys",
        type=array_of(struct_of(
            ("make", DataType.STRING),
            ("model", DataType.STRING),
            ("year", DataType.LONG),
            ("engine_model", DataType.STRING),
            ("engine_type", DataType.STRING),
            ("fuel_type", DataType.STRING),
        )),
        nullable=True,
        metadata=Metadata(comment="The possible MMYs for the device VIN."),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics VIN gaps.",
        row_meaning="Telematics VIN gaps for a device over a given time period. A result is a list of possible MMYs for the device VIN.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_VIN_ISSUES),
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_VIN_GAPS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_telematics_vin_gaps(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)