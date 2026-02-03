from typing import List, Set
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    ColumnType,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.query import (
    build_population_schema_header,
    format_agg_date_partition_query,
    generate_distinct_count_columns,
    create_run_config_overrides,
    verify_groupings_are_ordered,
    sort_groupings_by_logical_order,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

GROUPINGS: List[List[ColumnType]] = [
    [ColumnType.MARKET],
    [ColumnType.MARKET, ColumnType.DEVICE_TYPE],
    [ColumnType.MARKET, ColumnType.DEVICE_TYPE, ColumnType.ENGINE_TYPE],
    [ColumnType.MARKET, ColumnType.DEVICE_TYPE, ColumnType.ENGINE_TYPE, ColumnType.FUEL_TYPE, ColumnType.MAKE, ColumnType.MODEL, ColumnType.YEAR, ColumnType.ENGINE_MODEL],

    [ColumnType.MARKET, ColumnType.ORG_ID, ColumnType.DEVICE_TYPE],
    [ColumnType.MARKET, ColumnType.ORG_ID, ColumnType.DEVICE_TYPE, ColumnType.ENGINE_TYPE],

    [ColumnType.MARKET, ColumnType.DEVICE_TYPE, ColumnType.PRODUCT_NAME, ColumnType.VARIANT_NAME],
    [ColumnType.MARKET, ColumnType.DEVICE_TYPE, ColumnType.PRODUCT_NAME, ColumnType.VARIANT_NAME, ColumnType.CABLE_NAME, ColumnType.ENGINE_TYPE, ColumnType.FUEL_TYPE, ColumnType.MAKE, ColumnType.MODEL, ColumnType.YEAR, ColumnType.ENGINE_MODEL],
    [ColumnType.MARKET, ColumnType.DEVICE_TYPE, ColumnType.PRODUCT_NAME, ColumnType.CABLE_NAME, ColumnType.ENGINE_TYPE, ColumnType.FUEL_TYPE, ColumnType.MAKE, ColumnType.MODEL, ColumnType.YEAR, ColumnType.ENGINE_MODEL],
]

DIMENSIONS: List[ColumnType] = [
    ColumnType.MARKET,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_TYPE,
    ColumnType.PRODUCT_NAME,
    ColumnType.VARIANT_NAME,
    ColumnType.CABLE_NAME,
    ColumnType.ENGINE_TYPE,
    ColumnType.FUEL_TYPE,
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
]

GROUPINGS = sort_groupings_by_logical_order(GROUPINGS, DIMENSIONS)
verify_groupings_are_ordered(DIMENSIONS, GROUPINGS)

QUERY = """
WITH devices AS (
  SELECT
    date,
    device_id,
    {dimensions}
  FROM {product_analytics_staging}.dim_telematics_coverage_full
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
)

SELECT
  date,
  {grouping_hash},
  {grouping_label},
  {dimensions},
  {count_distinct_device_id},
  {count_distinct_columns},
  {grouping_level},
  COUNT_IF(engine_type = "ICE") AS count_ice,
  COUNT_IF(engine_type = "HYDROGEN") AS count_hydrogen,
  COUNT_IF(engine_type = "HYBRID") AS count_hybrid,
  COUNT_IF(engine_type = "BEV") AS count_bev,
  COUNT_IF(engine_type = "PHEV") AS count_phev,
  COUNT_IF(engine_type = "UNKNOWN") AS count_unknown
FROM devices
GROUP BY
  date,
  {grouping_sets}
"""

COLUMNS =  build_population_schema_header(
    columns=DIMENSIONS,
    aggregate_columns=generate_distinct_count_columns(DIMENSIONS + [ColumnType.DEVICE_ID]) + [
        ColumnType.GROUPING_LEVEL,
        ColumnType.COUNT_ICE,
        ColumnType.COUNT_HYDROGEN,
        ColumnType.COUNT_HYBRID,
        ColumnType.COUNT_BEV,
        ColumnType.COUNT_PHEV,
        ColumnType.COUNT_UNKNOWN,
    ],
    include_grouping_label=True,
)

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics population device grouping counts.",
        row_meaning="The grouped counts of population devices by grouping level.",
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
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    run_config_overrides=create_run_config_overrides(
        min_workers=1,
        max_workers=16,
        driver_instance_type=InstanceType.MD_FLEET_4XLARGE,
        worker_instance_type=InstanceType.MD_FLEET_4XLARGE,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_POPULATIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_populations(context: AssetExecutionContext) -> str:
    return format_agg_date_partition_query(
        context,
        QUERY,
        DIMENSIONS,
        GROUPINGS,
    )
