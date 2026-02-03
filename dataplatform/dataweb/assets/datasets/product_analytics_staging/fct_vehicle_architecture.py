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
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

QUERY = """
WITH messages_with_mmy AS (
  SELECT
    d.date,
    d.org_id,
    d.device_id,
    v.market,
    v.product_name,
    v.variant_name,
    v.cable_name,
    v.make,
    v.model,
    v.year,
    v.engine_model,
    v.fuel_type,
    v.engine_type,
    COLLECT_SET(
      NAMED_STRUCT(
        "bus_id", bus_id,
        "tx_id", tx_id,
        "message_id", message_id
      )
    ) AS diagnostic_messages_summary
  FROM {product_analytics_staging}.fct_osddiagnosticmessagesseen_classified d
  JOIN {product_analytics_staging}.dim_telematics_coverage_full v USING (date, org_id, device_id)
  WHERE date BETWEEN "{date_start}" AND "{date_end}"
  GROUP BY ALL
),

architecture_keys AS (
  SELECT
    date,
    org_id,
    device_id,
    market,
    product_name,
    variant_name,
    cable_name,
    make,
    model,
    year,
    engine_model,
    fuel_type,
    engine_type,
    diagnostic_messages_summary,
    SHA1(CAST(diagnostic_messages_summary AS STRING)) AS architecture_group_id
  FROM messages_with_mmy
)

SELECT
  date,
  architecture_group_id,
  market,
  product_name,
  cable_name,
  variant_name,
  make,
  model,
  year,
  engine_model,
  fuel_type,
  engine_type,
  diagnostic_messages_summary,
  COUNT(DISTINCT device_id) AS device_count
FROM architecture_keys
GROUP BY ALL
"""

COLUMNS = [
    ColumnType.DATE,
    Column(
        name="architecture_group_id",
        type=DataType.STRING,
        metadata=Metadata(
            comment="The architecture group id for the diagnostic messages."
        ),
        primary_key=True,
    ),
    replace(ColumnType.MARKET.value, primary_key=True),
    replace(ColumnType.PRODUCT_NAME.value, primary_key=True),
    replace(ColumnType.CABLE_NAME.value, primary_key=True),
    replace(ColumnType.VARIANT_NAME.value, primary_key=True),
    replace(ColumnType.MAKE.value, primary_key=True),
    replace(ColumnType.MODEL.value, primary_key=True),
    replace(ColumnType.YEAR.value, primary_key=True),
    replace(ColumnType.ENGINE_MODEL.value, primary_key=True),
    replace(ColumnType.FUEL_TYPE.value, primary_key=True),
    replace(ColumnType.ENGINE_TYPE.value, primary_key=True),
    Column(
        name="diagnostic_messages_summary",
        type=array_of(
            struct_of(
                ("bus_id", DataType.LONG),
                ("tx_id", DataType.LONG),
                ("message_id", DataType.LONG),
            ),
        ),
        nullable=True,
        metadata=Metadata(
            comment="Collected set of diagnostic message keys per bus_id / tx_id / message_id",
        ),
    ),
    Column(
        name="device_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="The number of devices that have the same architecture group id.",
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Group MMYs by diagnostic message architecture summary (bus_id, tx_id, message_id triplets).",
        row_meaning="Each row represents a unique MMY diagnostic architecture grouping and associated device count.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_OSD_DIAGNOSTIC_MESSAGES_SEEN_CLASSIFIED),
        AnyUpstream(ProductAnalyticsStaging.DIM_TELEMATICS_COVERAGE_FULL),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_VEHICLE_ARCHITECTURE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_vehicle_architecture(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        query=QUERY,
        context=context,
    )