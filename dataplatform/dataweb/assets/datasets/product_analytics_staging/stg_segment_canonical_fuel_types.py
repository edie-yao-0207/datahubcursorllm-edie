from enum import Enum

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import (
    Definitions,
    FuelDbShards,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)


COLUMNS = [
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="segment_start",
        type=DataType.TIMESTAMP,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment="Timestamp starting a segment.",
        ),
    ),
    Column(
        name="segment_end",
        type=DataType.TIMESTAMP,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment="Timestamp ending a segment.",
        ),
    ),
    Column(
        name="engine_type",
        type=DataType.BYTE,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific engine type enumeration.",
        ),
    ),
    Column(
        name="engine_type_name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific engine type name.",
        ),
    ),
    Column(
        name="gasoline_type",
        type=DataType.BYTE,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific gasoline type enumeration.",
        ),
    ),
    Column(
        name="gasoline_type_name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific gasoline type name.",
        ),
    ),
    Column(
        name="diesel_type",
        type=DataType.BYTE,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific diesel type enumeration.",
        ),
    ),
    Column(
        name="diesel_type_name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific diesel type name.",
        ),
    ),
    Column(
        name="gaseous_type",
        type=DataType.BYTE,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific gaseous type enumeration.",
        ),
    ),
    Column(
        name="gaseous_type_name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific gaseous type name.",
        ),
    ),
    Column(
        name="hydrogen_type",
        type=DataType.BYTE,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific hydrogen type enumeration.",
        ),
    ),
    Column(
        name="hydrogen_type_name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific hydrogen type name.",
        ),
    ),
    Column(
        name="battery_charge_type",
        type=DataType.BYTE,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific battery charge type enumeration.",
        ),
    ),
    Column(
        name="battery_charge_type_name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Canonical fuel type specific battery charge type name.",
        ),
    )
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = """
--sql

WITH segments AS (
  SELECT
      org_id
      , device_id
      , created_at AS segment_start
      , LEAD(created_at) OVER (PARTITION BY org_id, device_id ORDER BY created_at) AS segment_end
      , engine_type
      , gasoline_type
      , diesel_type
      , gaseous_type
      , hydrogen_type
      , battery_charge_type

  FROM
      fueldb_shards.fuel_types
)

SELECT
    org_id
    , device_id
    , segment_start
    , segment_end
    , engine_type
    , cft_engine.name AS engine_type_name
    , gasoline_type
    , CONCAT_WS(" ", COLLECT_SET(cft_gasoline.name)) AS gasoline_type_name
    , diesel_type
    , CONCAT_WS(" ", COLLECT_SET(cft_diesel.name)) AS diesel_type_name
    , gaseous_type
    , CONCAT_WS(" ", COLLECT_SET(cft_gaseous.name)) AS gaseous_type_name
    , hydrogen_type
    , CONCAT_WS(" ", COLLECT_SET(cft_hydrogen.name)) AS hydrogen_type_name
    , battery_charge_type
    , CONCAT_WS(" ", COLLECT_SET(cft_battery_charge.name)) AS battery_charge_type_name

FROM
  segments fuel_types

LEFT JOIN
    definitions.canonical_fuel_engine_type AS cft_engine
    ON fuel_types.engine_type = cft_engine.id

LEFT JOIN
    definitions.canonical_fuel_gasoline_type AS cft_gasoline
    ON (cft_gasoline.id & fuel_types.gasoline_type) = cft_gasoline.id
    AND (
      cft_gasoline.id != 0
      OR cft_gasoline.id = fuel_types.gasoline_type
    )

LEFT JOIN
    definitions.canonical_fuel_diesel_type AS cft_diesel
    ON (cft_diesel.id & fuel_types.diesel_type) = cft_diesel.id
    AND (
      cft_diesel.id != 0
      OR cft_diesel.id = fuel_types.diesel_type
    )

LEFT JOIN
    definitions.canonical_fuel_gaseous_type AS cft_gaseous
    ON (cft_gaseous.id & fuel_types.gaseous_type) = cft_gaseous.id
    AND (
      cft_gaseous.id != 0
      OR cft_gaseous.id = fuel_types.gaseous_type
    )

LEFT JOIN
    definitions.canonical_fuel_hydrogen_type AS cft_hydrogen
    ON (cft_hydrogen.id & fuel_types.hydrogen_type) = cft_hydrogen.id
    AND (
      cft_hydrogen.id != 0
      OR cft_hydrogen.id = fuel_types.hydrogen_type
    )

LEFT JOIN
    definitions.canonical_fuel_battery_charge_type AS cft_battery_charge
    ON (cft_battery_charge.id & fuel_types.battery_charge_type) = cft_battery_charge.id
    AND (
      cft_battery_charge.id != 0
      OR cft_battery_charge.id = fuel_types.battery_charge_type
    )


GROUP BY ALL

--endsql
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Segments of canonical fuel types data per device.",
        row_meaning="Fuel type for each device",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(FuelDbShards.FUEL_TYPES),
        AnyUpstream(Definitions.CANONICAL_FUEL_GASOLINE_TYPE),
        AnyUpstream(Definitions.CANONICAL_FUEL_DIESEL_TYPE),
        AnyUpstream(Definitions.CANONICAL_FUEL_GASEOUS_TYPE),
        AnyUpstream(Definitions.CANONICAL_FUEL_HYDROGEN_TYPE),
        AnyUpstream(Definitions.CANONICAL_FUEL_ENGINE_TYPE),
        AnyUpstream(Definitions.CANONICAL_FUEL_BATTERY_CHARGE_TYPE),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.STG_SEGMENT_CANONICAL_FUEL_TYPES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def stg_segment_canonical_fuel_types(context: AssetExecutionContext) -> str:
    return QUERY
