"""
Device-level diagnostics coverage data queryable by VIN

Device-specific signal coverage facts enabling VIN-based diagnostics lookups.
Unlike the population-level view (dim_diagnostics_coverage_v3), this table provides
coverage data for individual devices filtered by VIN for customer-specific diagnostics.

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
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    struct_from_columns,
    map_of,
)
from dataweb.userpkgs.firmware.table import (
    ProductAnalytics,
    ProductAnalyticsStaging,
    Definitions,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description


# Device-level coverage struct columns (boolean coverage values)
DEVICE_COVERAGE_STRUCT_COLUMNS = [
    Column(
        name="turn_signal_detected",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Turn signal support on this device (true/false)"),
    ),
    Column(
        name="reverse_gear_detected",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Reverse gear support on this device (true/false)"),
    ),
    Column(
        name="door_status_detected",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="Door status detected on this device (true/false)"),
    ),
    Column(
        name="can_connected_detected",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(comment="CAN connected detected on this device (true/false)"),
    ),
    Column(
        name="turn_signal_recommendation",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Turn signal install recommendation"),
    ),
    Column(
        name="reverse_gear_recommendation",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Reverse gear install recommendation"),
    ),
    Column(
        name="door_status_recommendation",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Door status install recommendation"),
    ),
    Column(
        name="aim4_recommendation",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Consolidated AIM4 recommendation (worst case of turn signal + reverse gear)"
        ),
    ),
]

DEVICE_COVERAGE_STRUCT = struct_from_columns(*DEVICE_COVERAGE_STRUCT_COLUMNS)

# Schema definition
COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="vin",
        type=DataType.STRING,
        primary_key=True,
        nullable=False,
        metadata=Metadata(comment="Vehicle Identification Number"),
    ),
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
    Column(
        name="powertrain_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Powertrain type (ICE, BEV, PHEV, HYBRID, UNKNOWN)"),
    ),
    Column(
        name="fuel_group_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Fuel group (DIESEL, GASOLINE, ELECTRICITY, GASEOUS, HYDROGEN, UNKNOWN)"
        ),
    ),
    ColumnType.TRIM,
    ColumnType.MARKET,
    Column(
        name="product_variant_name",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Product variant name"),
    ),
    ColumnType.PRODUCT_NAME,
    ColumnType.CABLE_NAME,
    Column(
        name="pre_vg36_fw",
        type=DEVICE_COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="pre-vg-36 baseline coverage using j1939_oel_turn_signal_switch_state and vehicle_current_gear"
        ),
    ),
    Column(
        name="vg36_fw",
        type=DEVICE_COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="vg-36 coverage: adds SPN 2367/2369 (left/right turn signal) support"
        ),
    ),
    Column(
        name="vg37_fw",
        type=DEVICE_COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="vg-37 target coverage: adds SPN 767 (reverse direction) and SPN 1619 (direction indicator)"
        ),
    ),
    Column(
        name="final_recommendation_explanation",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Combined recommendation and improvement explanation for this device"
        ),
    ),
    Column(
        name="coverage_map",
        type=map_of(DataType.STRING, DataType.BOOLEAN, value_contains_null=True),
        nullable=True,
        metadata=Metadata(
            comment="Map of signal type to support boolean for all tracked signals on this device"
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = r"""
WITH

-- Step 1: Get the max date that exists in ALL source tables
max_install_date AS (
    SELECT MAX(date) as max_date
    FROM product_analytics_staging.fct_telematics_install_recommendations
),
max_coverage_date AS (
    SELECT MAX(date) as max_date
    FROM product_analytics_staging.fct_telematics_coverage_rollup_full
),
max_device_props_date AS (
    SELECT MAX(date) as max_date
    FROM product_analytics_staging.dim_device_vehicle_properties
),
max_vin_issues_date AS (
    SELECT MAX(date) as max_date
    FROM product_analytics_staging.fct_telematics_vin_issues
),
max_device_dimensions_date AS (
    SELECT MAX(date) as max_date
    FROM product_analytics.dim_device_dimensions
),
common_max_date AS (
    SELECT
        LEAST(
            (SELECT max_date FROM max_install_date),
            (SELECT max_date FROM max_coverage_date),
            (SELECT max_date FROM max_device_props_date),
            (SELECT max_date FROM max_vin_issues_date),
            (SELECT max_date FROM max_device_dimensions_date)
        ) as max_date
),

-- Step 2: Get coverage data with filters
coverage_base AS (
    SELECT
        cover.date,
        cover.org_id,
        cover.device_id,
        cover.type,
        cover.day_covered
    FROM product_analytics_staging.fct_telematics_coverage_rollup_full AS cover
    WHERE cover.date = (SELECT max_date FROM common_max_date)
),

-- Step 3: Get install recommendations data
install_recommendations AS (
    SELECT
        inst.date,
        inst.org_id,
        inst.device_id,
        inst.pre_vg36_fw,
        inst.vg36_fw,
        inst.vg37_fw,
        inst.vg_improvement_reason,
        inst.recommendation_explanation
    FROM product_analytics_staging.fct_telematics_install_recommendations AS inst
    WHERE inst.date = (SELECT max_date FROM common_max_date)
),

-- Step 4: Get device vehicle properties
device_properties AS (
    SELECT
        dim.date,
        dim.org_id,
        dim.device_id,
        dim.make,
        dim.model,
        dim.year,
        dim.engine_model,
        dim.fuel_group,
        dim.powertrain,
        dim.trim
    FROM product_analytics_staging.dim_device_vehicle_properties AS dim
    WHERE
        dim.date = (SELECT max_date FROM common_max_date)
        AND dim.make != 'UNKNOWN'
        AND dim.model != 'UNKNOWN'
        AND dim.year != -1
),

-- Step 5: Get VIN issues data
vin_issues AS (
    SELECT
        vin_issues.date,
        vin_issues.org_id,
        vin_issues.device_id,
        vin_issues.vin
    FROM product_analytics_staging.fct_telematics_vin_issues AS vin_issues
    WHERE vin_issues.date = (SELECT max_date FROM common_max_date) AND vin_issues.vin IS NOT NULL
),

-- Step 5b: Get device dimensions data
device_dimensions AS (
    SELECT
        dd.date,
        dd.org_id,
        dd.device_id,
        dd.market,
        dd.variant_name,
        dd.product_name,
        dd.cable_name
    FROM product_analytics.dim_device_dimensions AS dd
    WHERE
        dd.date = (SELECT max_date FROM common_max_date)
        AND dd.cable_name IS NOT NULL
        AND dd.cable_name NOT IN ('UNKNOWN OR POWER ONLY', 'Passenger')
),

-- Step 6: Join coverage with install recommendations
coverage_with_installs AS (
    SELECT
        cover.date,
        cover.org_id,
        cover.device_id,
        cover.type,
        cover.day_covered,
        inst.pre_vg36_fw,
        inst.vg36_fw,
        inst.vg37_fw,
        inst.vg_improvement_reason,
        inst.recommendation_explanation
    FROM coverage_base AS cover
    LEFT JOIN install_recommendations AS inst
        ON cover.date = inst.date
        AND cover.org_id = inst.org_id
        AND cover.device_id = inst.device_id
),

-- Step 7: Join with device properties
coverage_with_device_props AS (
    SELECT
        cover.date,
        cover.org_id,
        cover.device_id,
        cover.type,
        cover.day_covered,
        cover.pre_vg36_fw,
        cover.vg36_fw,
        cover.vg37_fw,
        cover.vg_improvement_reason,
        cover.recommendation_explanation,
        dim.make,
        dim.model,
        dim.year,
        dim.engine_model,
        dim.fuel_group,
        dim.powertrain,
        dim.trim
    FROM coverage_with_installs AS cover
    LEFT JOIN device_properties AS dim
        ON cover.date = dim.date
        AND cover.org_id = dim.org_id
        AND cover.device_id = dim.device_id
),

-- Step 8: Join with fuel group definitions
coverage_with_fuel_group AS (
    SELECT
        cover.date,
        cover.org_id,
        cover.device_id,
        cover.type,
        cover.day_covered,
        cover.pre_vg36_fw,
        cover.vg36_fw,
        cover.vg37_fw,
        cover.vg_improvement_reason,
        cover.recommendation_explanation,
        cover.make,
        cover.model,
        cover.year,
        cover.engine_model,
        cover.powertrain,
        cover.trim,
        CASE
            WHEN fuel_group_def.name LIKE '%DIESEL%' THEN 'DIESEL'
            WHEN fuel_group_def.name LIKE '%GASOLINE%' THEN 'GASOLINE'
            WHEN fuel_group_def.name LIKE '%ELECTRICITY%' THEN 'ELECTRICITY'
            WHEN fuel_group_def.name LIKE '%GASEOUS%' THEN 'GASEOUS'
            WHEN fuel_group_def.name LIKE '%HYDROGEN%' THEN 'HYDROGEN'
            ELSE 'UNKNOWN'
        END as fuel_group_type
    FROM coverage_with_device_props AS cover
    LEFT JOIN definitions.properties_fuel_fuelgroup AS fuel_group_def
        ON cover.fuel_group = fuel_group_def.id
),

-- Step 9: Join with powertrain definitions
coverage_with_powertrain AS (
    SELECT
        cover.date,
        cover.org_id,
        cover.device_id,
        cover.type,
        cover.day_covered,
        cover.pre_vg36_fw,
        cover.vg36_fw,
        cover.vg37_fw,
        cover.vg_improvement_reason,
        cover.recommendation_explanation,
        cover.make,
        cover.model,
        cover.year,
        cover.engine_model,
        cover.fuel_group_type,
        cover.trim,
        CASE
            WHEN powertrain_def.name LIKE '%INTERNAL_COMBUSTION_ENGINE%' THEN 'ICE'
            WHEN powertrain_def.name LIKE '%BATTERY_ELECTRIC_VEHICLE%' THEN 'BEV'
            WHEN powertrain_def.name LIKE '%PLUG_IN_HYBRID%' THEN 'PHEV'
            WHEN powertrain_def.name LIKE '%HYBRID%' THEN 'HYBRID'
            ELSE 'UNKNOWN'
        END as powertrain_type
    FROM coverage_with_fuel_group AS cover
    LEFT JOIN definitions.properties_fuel_powertrain AS powertrain_def
        ON cover.powertrain = powertrain_def.id
),

-- Step 10: Join with VIN issues (INNER JOIN to only keep devices with VINs)
coverage_with_vin AS (
    SELECT
        cover.date,
        cover.org_id,
        cover.device_id,
        cover.type,
        cover.day_covered,
        cover.make,
        cover.model,
        cover.year,
        cover.engine_model,
        cover.powertrain_type,
        cover.fuel_group_type,
        cover.trim,
        cover.pre_vg36_fw,
        cover.vg36_fw,
        cover.vg37_fw,
        cover.vg_improvement_reason,
        cover.recommendation_explanation,
        vin.vin
    FROM coverage_with_powertrain AS cover
    INNER JOIN vin_issues AS vin
        ON cover.date = vin.date
        AND cover.org_id = vin.org_id
        AND cover.device_id = vin.device_id
),

-- Step 11: Join with device dimensions (INNER JOIN to filter by cable_name)
complete_dataset AS (
    SELECT
        cover.date,
        cover.org_id,
        cover.device_id,
        cover.type,
        cover.day_covered,
        cover.make,
        cover.model,
        cover.year,
        cover.engine_model,
        cover.powertrain_type,
        cover.fuel_group_type,
        cover.trim,
        cover.pre_vg36_fw,
        cover.vg36_fw,
        cover.vg37_fw,
        cover.vg_improvement_reason,
        cover.recommendation_explanation,
        cover.vin,
        dd.market,
        dd.variant_name,
        dd.product_name,
        dd.cable_name
    FROM coverage_with_vin AS cover
    INNER JOIN device_dimensions AS dd
        ON cover.date = dd.date
        AND cover.org_id = dd.org_id
        AND cover.device_id = dd.device_id
)

-- Step 12: Final aggregation by device with coverage map
SELECT
    date,
    org_id,
    device_id,
    vin,
    make,
    model,
    year,
    engine_model,
    powertrain_type,
    fuel_group_type,
    trim,
    market,
    variant_name as product_variant_name,
    product_name,
    cable_name,
    MAX(pre_vg36_fw) as pre_vg36_fw,
    MAX(vg36_fw) as vg36_fw,
    MAX(vg37_fw) as vg37_fw,
    CONCAT(
        COALESCE(MAX(recommendation_explanation), ''),
        CASE
            WHEN MAX(recommendation_explanation) IS NOT NULL
                 AND MAX(vg_improvement_reason) IS NOT NULL
            THEN ' '
            ELSE ''
        END,
        COALESCE(MAX(vg_improvement_reason), '')
    ) as final_recommendation_explanation,
    MAP_FROM_ENTRIES(
        COLLECT_LIST(
            STRUCT(type, day_covered)
        )
    ) as coverage_map
FROM complete_dataset
GROUP BY
    date,
    org_id,
    device_id,
    vin,
    make,
    model,
    year,
    engine_model,
    powertrain_type,
    fuel_group_type,
    trim,
    market,
    variant_name,
    product_name,
    cable_name
ORDER BY device_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Device-level diagnostics coverage data queryable by VIN. Powers VIN-based diagnostics coverage lookups in GraphQL API. Unlike population-level views, this provides coverage for individual devices.",
        row_meaning="Each row represents diagnostics signal coverage for one device with VIN metadata and firmware version progression.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    single_run_backfill=True,
    partitioning=None,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_COVERAGE_ROLLUP_FULL),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_INSTALL_RECOMMENDATIONS),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_VIN_ISSUES),
        AnyUpstream(ProductAnalytics.DIM_DEVICE_DIMENSIONS),
        AnyUpstream(Definitions.PROPERTIES_FUEL_FUELGROUP),
        AnyUpstream(Definitions.PROPERTIES_FUEL_POWERTRAIN),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name="fct_diagnostics_coverage_by_vin_latest",
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_diagnostics_coverage_by_vin_latest(context: AssetExecutionContext) -> str:
    """
    Generate device-level diagnostics coverage facts queryable by VIN.

    This table provides signal coverage data for individual devices with VIN metadata,
    enabling vehicle diagnostics coverage tool (samsara.com/vehicle_diagnostics_v2).

    Unlike population-level views (dim_diagnostics_coverage_v3), this table:
    - Provides coverage for specific devices/VINs
    - Uses boolean coverage values (true/false) instead of percentages
    """
    return QUERY
