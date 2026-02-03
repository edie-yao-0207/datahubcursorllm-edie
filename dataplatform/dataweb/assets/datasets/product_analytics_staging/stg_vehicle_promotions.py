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
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

COLUMNS = [
    replace(ColumnType.MAKE.value, primary_key=True, nullable=False),
    replace(ColumnType.MODEL.value, primary_key=True, nullable=False),
    replace(ColumnType.YEAR.value, primary_key=True, nullable=False),
    replace(ColumnType.ENGINE_MODEL.value, primary_key=True, nullable=True),
    replace(ColumnType.PRIMARY_FUEL_TYPE.value, primary_key=True, nullable=True),
    replace(ColumnType.SIGNAL.value, primary_key=True, nullable=False),
    replace(ColumnType.BUS.value, primary_key=True, nullable=False),
    Column(
        name="request_address",
        type=DataType.LONG,
        nullable=True,
        primary_key=True,
        metadata=Metadata(comment="Request address."),
    ),
    Column(
        name="response_address",
        type=DataType.LONG,
        nullable=True,
        primary_key=True,
        metadata=Metadata(comment="Response address."),
    ),
    Column(
        name="data_identifier",
        type=DataType.LONG,
        nullable=True,
        primary_key=True,
        metadata=Metadata(comment="Data identifier."),
    ),
    Column(
        name="parser",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="Parser used."),
    ),
    Column(
        name="validator",
        type=DataType.STRING,
        nullable=True,
        primary_key=True,
        metadata=Metadata(comment="Validator used."),
    ),
    replace(ColumnType.ELECTRIFICATION_LEVEL.value, primary_key=True),
    Column(
        name="disable_firmware_vin_check",
        type=DataType.BOOLEAN,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment="This field tells us if VIN checks are required."
        ),
    ),
    Column(
        name="primary_trim",
        type=DataType.STRING,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment="Primary trim designation for the vehicle."
        ),
    ),
    Column(
        name="secondary_trim",
        type=DataType.STRING,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment="Secondary trim designation for the vehicle."
        ),
    ),
]

QUERY = """
SELECT
    make,
    model,
    EXPLODE(
        SEQUENCE(
            CASE
                WHEN year_start = 0 OR year_start IS NULL THEN 2008
                WHEN year_start < 1900 THEN NULL
                WHEN year_start <= YEAR(CURRENT_DATE()) + 4 THEN year_start
                ELSE YEAR(CURRENT_DATE()) + 4
            END,
            CASE
                WHEN year_end = 0 OR year_end IS NULL THEN YEAR(CURRENT_DATE()) + 2
                WHEN year_end < 1900 THEN NULL
                WHEN year_end <= YEAR(CURRENT_DATE()) + 4 THEN year_end
                ELSE YEAR(CURRENT_DATE()) + 4
            END
        )
    ) AS year,
    engine_model,
    CASE
        WHEN fuel_type = "FUEL_TYPE_UNKNOWN" THEN NULL
        ELSE REPLACE(fuel_type, "FUEL_TYPE_", "")
    END AS primary_fuel_type,
    obd_value_enum AS signal,
    vehicle_diagnostic_bus AS bus,
    module_address AS request_address,
    response_address,
    data_identifier,
    parser,
    validator,
    CASE
        WHEN vehicle_electrification_type = "ANY" THEN NULL
        ELSE vehicle_electrification_type
    END AS electrification_level,
    disable_firmware_vin_check,
    trim AS primary_trim,
    trim_secondary AS secondary_trim
FROM
    definitions.promotions
"""

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily definition promotions data from the backend (config builder) source.",
        row_meaning="Promotions definitions from the backend",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(Definitions.PROMOTIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.STG_VEHICLE_PROMOTIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def stg_vehicle_promotions(context: AssetExecutionContext) -> str:
    return QUERY
