"""
Aggregated diagnostics coverage metrics for vehicle populations

Population-level aggregation of diagnostics signal coverage and installation recommendations
organized by vehicle configuration (make, model, year, engine_model, fuel_group, etc.).
This table powers the vehicle diagnostics coverage tool (samsara.com/vehicle_diagnostics_v2)

Firmware version structs (pre_vg36_fw, vg36_fw, vg37_fw) show coverage evolution across releases.
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
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description


# Coverage struct columns (reused across firmware versions)
COVERAGE_STRUCT_COLUMNS = [
    Column(
        name="turn_signal_coverage",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Turn signal coverage ratio (0.0-1.0)"),
    ),
    Column(
        name="reverse_gear_coverage",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Reverse gear coverage ratio (0.0-1.0)"),
    ),
    Column(
        name="door_status_coverage",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="Door status coverage ratio (0.0-1.0)"),
    ),
    Column(
        name="can_connected_coverage",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(comment="CAN connected coverage ratio (0.0-1.0)"),
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

COVERAGE_STRUCT = struct_from_columns(*COVERAGE_STRUCT_COLUMNS)

# Schema definition
COLUMNS = [
    ColumnType.DATE,
    ColumnType.MAKE,
    ColumnType.MODEL,
    Column(
        name="year",
        type=DataType.LONG,
        primary_key=True,
        nullable=False,
        metadata=Metadata(comment="Vehicle year"),
    ),
    ColumnType.ENGINE_MODEL,
    Column(
        name="powertrain_type",
        type=DataType.STRING,
        primary_key=True,
        nullable=False,
        metadata=Metadata(comment="Powertrain type (ICE, BEV, PHEV, HYBRID, UNKNOWN)"),
    ),
    Column(
        name="fuel_group_type",
        type=DataType.STRING,
        primary_key=True,
        nullable=False,
        metadata=Metadata(
            comment="Fuel group (DIESEL, GASOLINE, ELECTRICITY, GASEOUS, HYDROGEN, UNKNOWN)"
        ),
    ),
    Column(
        name="market",
        type=DataType.STRING,
        primary_key=True,
        nullable=False,
        metadata=Metadata(comment="Geographic market (NA, EU, etc.)"),
    ),
    Column(
        name="product_name",
        type=DataType.STRING,
        primary_key=True,
        nullable=False,
        metadata=Metadata(comment="Product variant name"),
    ),
    Column(
        name="cable_name",
        type=DataType.STRING,
        primary_key=True,
        nullable=False,
        metadata=Metadata(comment="OBD cable type"),
    ),
    ColumnType.GROUPING_HASH,
    Column(
        name="distinct_device_id_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Number of devices with this configuration"),
    ),
    Column(
        name="distinct_org_id_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Number of organizations with this configuration"),
    ),
    Column(
        name="most_frequent_install",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="True if this is the most common install for this vehicle"
        ),
    ),
    Column(
        name="recommendation_confidence",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Confidence level: Low (<50 devices OR 1 org <200 devices), Medium (50-200 devices multiple orgs OR 200+ devices 1 org), High (200+ devices multiple orgs)"
        ),
    ),
    Column(
        name="pre_vg36_fw",
        type=COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="Pre vg-36 baseline coverage using j1939_oel_turn_signal_switch_state and vehicle_current_gear"
        ),
    ),
    Column(
        name="vg36_fw",
        type=COVERAGE_STRUCT,
        nullable=True,
        metadata=Metadata(
            comment="vg-36 coverage: adds SPN 2367/2369 (left/right turn signal) support"
        ),
    ),
    Column(
        name="vg37_fw",
        type=COVERAGE_STRUCT,
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
            comment="Combined recommendation and improvement explanation for this vehicle configuration"
        ),
    ),
    Column(
        name="coverage_map",
        type=map_of(DataType.STRING, DataType.DOUBLE, value_contains_null=True),
        nullable=True,
        metadata=Metadata(
            comment="Map of signal type to coverage percentage for all tracked signals"
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

QUERY = r"""
WITH
-- Step 1: Get the max date that exists in BOTH source tables
max_dates AS (
    SELECT
        MAX(date) as max_install_date
    FROM product_analytics_staging.agg_telematics_install_recommendations
),
max_populations_date AS (
    SELECT
        MAX(date) as max_pop_date
    FROM product_analytics_staging.agg_telematics_populations
),
max_coverage_date AS (
    SELECT
        MAX(date) as max_cov_date
    FROM product_analytics_staging.agg_telematics_actual_coverage_normalized
),
common_max_date AS (
    SELECT
        LEAST(
            (SELECT max_install_date FROM max_dates),
            (SELECT max_pop_date FROM max_populations_date),
            (SELECT max_cov_date FROM max_coverage_date)
        ) as max_date
),

-- Step 2: Get populations data with filters and transformations
populations_base AS (
    SELECT
        pop.date,
        pop.make,
        pop.model,
        pop.year,
        pop.engine_model,
        pop.engine_type as powertrain_type,
        pop.fuel_type,
        pop.market,
        pop.device_type,
        pop.product_name,
        pop.variant_name,
        pop.cable_name,
        pop.grouping_hash
    FROM product_analytics_staging.agg_telematics_populations AS pop
    WHERE
        pop.date = (SELECT max_date FROM common_max_date)
        AND pop.grouping_level = 'market + device_type + product_name + variant_name + cable_name + engine_type + fuel_type + make + model + year + engine_model'
        AND pop.device_type = 'VG'
        AND pop.make != 'UNKNOWN'
        AND pop.model != 'UNKNOWN'
        AND pop.year != -1
),

-- Step 3: Transform fuel_type into fuel_group_type
populations_with_fuel_group AS (
    SELECT
        date,
        make,
        model,
        year,
        engine_model,
        powertrain_type,
        CASE
            WHEN fuel_type LIKE '%Diesel%' OR fuel_type = 'Biodiesel' THEN 'DIESEL'
            WHEN fuel_type LIKE '%Gasoline%' OR fuel_type = 'E85' THEN 'GASOLINE'
            WHEN fuel_type LIKE '%Plugin%' OR fuel_type LIKE '%Chargeable%' THEN 'ELECTRICITY'
            WHEN fuel_type IN ('CNG', 'LNG', 'LPG', 'NaturalGas')
                 OR fuel_type LIKE '%CNG%'
                 OR fuel_type LIKE '%LNG%'
                 OR fuel_type LIKE '%LPG%' THEN 'GASEOUS'
            WHEN fuel_type LIKE '%Hydrogen%' THEN 'HYDROGEN'
            ELSE 'UNKNOWN'
        END as fuel_group_type,
        market,
        device_type,
        product_name,
        variant_name,
        cable_name,
        grouping_hash
    FROM populations_base
),

-- Step 4: Get coverage data
coverage_data AS (
    SELECT
        cover.date,
        cover.grouping_hash,
        cover.type,
        COALESCE(cover.percent_coverage, 0) as percent_coverage
    FROM product_analytics_staging.agg_telematics_actual_coverage_normalized AS cover
    WHERE cover.date = (SELECT max_date FROM common_max_date)
),

-- Step 5: Get install recommendations data
install_recommendations AS (
    SELECT
        inst.date,
        inst.grouping_hash,
        inst.count_distinct_device_id as distinct_device_id_count,
        inst.count_distinct_org_id as distinct_org_id_count,
        inst.most_frequent_install,
        inst.recommendation_confidence,
        inst.pre_vg36_fw,
        inst.vg36_fw,
        inst.vg37_fw,
        inst.vg_improvement_reason,
        inst.recommendation_explanation
    FROM product_analytics_staging.agg_telematics_install_recommendations AS inst
    WHERE inst.date = (SELECT max_date FROM common_max_date)
),

-- Step 6: Join populations with install recommendations
populations_with_installs AS (
    SELECT
        pop.date,
        pop.make,
        pop.model,
        pop.year,
        pop.engine_model,
        pop.powertrain_type,
        pop.fuel_group_type,
        pop.market,
        pop.device_type,
        pop.product_name,
        pop.variant_name,
        pop.cable_name,
        pop.grouping_hash,
        inst.distinct_device_id_count,
        inst.distinct_org_id_count,
        inst.most_frequent_install,
        inst.recommendation_confidence,
        inst.pre_vg36_fw,
        inst.vg36_fw,
        inst.vg37_fw,
        inst.vg_improvement_reason,
        inst.recommendation_explanation
    FROM populations_with_fuel_group AS pop
    LEFT JOIN install_recommendations AS inst
        ON pop.date = inst.date
        AND pop.grouping_hash = inst.grouping_hash
),

-- Step 7: Join with coverage data
complete_dataset AS (
    SELECT
        pop.date,
        pop.make,
        pop.model,
        pop.year,
        pop.engine_model,
        pop.powertrain_type,
        pop.fuel_group_type,
        pop.market,
        pop.device_type,
        pop.product_name,
        pop.variant_name,
        pop.cable_name,
        pop.grouping_hash,
        pop.distinct_device_id_count,
        pop.distinct_org_id_count,
        pop.most_frequent_install,
        pop.recommendation_confidence,
        pop.pre_vg36_fw,
        pop.vg36_fw,
        pop.vg37_fw,
        pop.vg_improvement_reason,
        pop.recommendation_explanation,
        cover.type,
        cover.percent_coverage
    FROM populations_with_installs AS pop
    LEFT JOIN coverage_data AS cover
        ON pop.date = cover.date
        AND pop.grouping_hash = cover.grouping_hash
)

-- Step 8: Final aggregation with coverage map
SELECT
    date,
    make,
    model,
    year,
    engine_model,
    powertrain_type,
    fuel_group_type,
    market,
    variant_name as product_name,
    cable_name,
    grouping_hash,
    MAX(distinct_device_id_count) as distinct_device_id_count,
    MAX(distinct_org_id_count) as distinct_org_id_count,
    MAX(most_frequent_install) as most_frequent_install,
    MAX(recommendation_confidence) as recommendation_confidence,
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
            STRUCT(type, percent_coverage)
        )
    ) as coverage_map
FROM complete_dataset
WHERE cable_name IS NOT NULL AND cable_name NOT IN ('UNKNOWN OR POWER ONLY', 'Passenger')
GROUP BY
    date,
    make,
    model,
    year,
    engine_model,
    powertrain_type,
    fuel_group_type,
    market,
    device_type,
    product_name,
    variant_name,
    cable_name,
    grouping_hash
ORDER BY distinct_device_id_count DESC
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Aggregated diagnostics coverage metrics organized by vehicle configuration with firmware version progression. Powers the vehicle diagnostics coverage tool (samsara.com/vehicle_diagnostics_v2)",
        row_meaning="Each row represents aggregated diagnostics signal coverage and installation recommendations for one vehicle population (grouping_hash) with coverage data organized by firmware version.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=None,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_POPULATIONS),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_ACTUAL_COVERAGE_NORMALIZED),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_INSTALL_RECOMMENDATIONS),
    ],
    single_run_backfill=True,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name="agg_diagnostics_coverage_v3_latest",
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_diagnostics_coverage_v3_latest(context: AssetExecutionContext) -> str:
    """
    Generate aggregated diagnostics coverage metrics by vehicle population (MMYEFPC).

    This table aggregates signal coverage and installation recommendations for vehicle
    populations, enabling diagnostics coverage tool (samsara.com/vehicle_diagnostics_v2).

    Coverage is organized by firmware version (pre_vg36_fw, vg36_fw, vg37_fw) to show
    the progression of signal support across releases.
    """
    return QUERY
