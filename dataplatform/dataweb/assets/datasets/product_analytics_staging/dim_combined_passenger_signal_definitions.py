"""
dim_combined_passenger_signal_definitions

Combined passenger signal definitions with comprehensive OBD mappings and VDP compatibility.
Loads and processes three signal definition sources:
1. J1979 DA Annex B signals from CSV with VDP format transformations
2. Global OBD configuration signals with MMYEF (Make/Model/Year/Engine/Fuel) matching
3. Passenger standard emitter signals from definitions tables
Combines all sources for comprehensive automotive diagnostic signal reference data.
"""

from dataclasses import replace

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.can.can_recompiler import EndiannessType, SignType
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import Definitions, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, lit, row_number, when
from pyspark.sql.window import Window

from .dim_combined_signal_definitions import SignalSourceType
from .dim_device_vehicle_properties import MMYEF_HASH_EXPRESSION, MMYEF_ID

ROW_ID_COLUMN_BASE = Column(
    name="row_id",
    type=DataType.LONG,
    primary_key=True,
    metadata=Metadata(
        comment="Unique numeric identifier for each entry in the combined passenger signal definitions table."
    ),
)
COMMENT_COLUMN = Column(
    name="comment",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Detailed description of the signal."),
)
SOURCE_COLUMN = Column(
    name="source",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="Source of the signal definition."),
)

COLUMNS = [
    ROW_ID_COLUMN_BASE,
    ColumnType.DATA_IDENTIFIER,
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
    replace(ColumnType.POWERTRAIN.value, primary_key=False),
    replace(ColumnType.FUEL_GROUP.value, primary_key=False),
    ColumnType.TRIM,
    MMYEF_ID,
    ColumnType.BIT_START,
    ColumnType.BIT_LENGTH,
    ColumnType.ENDIAN,
    ColumnType.SCALE,
    ColumnType.SIGN,
    ColumnType.OFFSET,
    ColumnType.MINIMUM,
    ColumnType.MAXIMUM,
    ColumnType.INTERNAL_SCALING,
    ColumnType.UNIT,
    ColumnType.OBD_VALUE,
    ColumnType.PASSIVE_RESPONSE_MASK,
    ColumnType.RESPONSE_ID,
    ColumnType.REQUEST_ID,
    COMMENT_COLUMN,
    ColumnType.DATA_SOURCE_ID,
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Enhanced passenger signal definitions with partial OBD mappings. "
        "Loads authoritative J1979, passenger global obd, and passenger standard emitter signal definitions from definitions "
        "tables, applies VDP format transformations for consistent interpretation, and combines the signal sources for "
        "comprehensive signal reference data.",
        row_meaning="Each row represents a processed passenger signal definition for a specific PID with VDP-compatible "
        "formatting and optional OBD value mappings.",
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(Definitions.PASSENGER_STANDARD_EMITTER_SIGNALS),
        AnyUpstream(Definitions.J1979_DA_ANNEX_B_2025_04),
        AnyUpstream(Definitions.GLOBAL_OBD_CONFIG_PASSENGER_SIGNAL_DEFS),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_COMBINED_PASSENGER_SIGNAL_DEFINITIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def dim_combined_passenger_signal_definitions(context: AssetExecutionContext) -> DataFrame:
    """
    Load and process multiple passenger signal definition sources with VDP compatibility and OBD mappings.

    This asset performs comprehensive signal definition processing:
    1. Loads J1979 DA Annex B signal definitions from definitions table with VDP format transformations
    2. Loads global OBD configuration signals from definitions table with fuel type and electrification mappings
    3. Expands global OBD signals by matching against vehicle properties using MMYEF patterns
    4. Loads passenger standard emitter signals from definitions tables
    5. Merges J1979 signals with standard emitter signals, replacing duplicates with standard emitter versions
    6. Combines all processed sources into a unified dataset for comprehensive automotive diagnostic signal reference

    Returns a DataFrame with signal definitions from all sources, formatted for VDP compatibility.
    """
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    context.log.info("Loading J1979 signals from definitions table with VDP processing")

    j1979_df = spark.sql("SELECT * FROM definitions.j1979_da_annex_b_2025_04")

    context.log.info(f"Loaded {j1979_df.count():,} rows from J1979 signal definitions")

    global_obd_df = spark.sql("SELECT * FROM definitions.global_obd_config_passenger_signal_defs")

    context.log.info(f"Loaded {global_obd_df.count():,} rows from Global OBD config")

    # Map electrification_type to powertrain, with "ANY" kept as NULL for wildcard matching
    global_obd_df = global_obd_df.withColumn(
        ColumnType.POWERTRAIN.value.name,
        # PowerTrain enum values from vehicle_property_service.proto
        when(col("electrification_type") == "ANY", lit(None).cast(ColumnType.POWERTRAIN.value.type))  # NULL for wildcard
        .when(col("electrification_type") == "BEV", lit(2).cast(ColumnType.POWERTRAIN.value.type))
        .when(col("electrification_type") == "PHEV", lit(3).cast(ColumnType.POWERTRAIN.value.type))
        .when(col("electrification_type") == "HYBRID", lit(4).cast(ColumnType.POWERTRAIN.value.type))
        .when(col("electrification_type") == "FCEV", lit(6).cast(ColumnType.POWERTRAIN.value.type))
        .otherwise(lit(0).cast(ColumnType.POWERTRAIN.value.type)) # TODO map other electrification types
    )

    # Process all rows to map fuel_type to fuel_group, with FUEL_TYPE_UNKNOWN kept as NULL for wildcard matching
    global_obd_df = global_obd_df.withColumn(
        ColumnType.FUEL_GROUP.value.name,
        # FuelGroup enum values from vehicle_property_service.proto
        when(col("fuel_type") == "FUEL_TYPE_UNKNOWN", lit(None).cast(ColumnType.FUEL_GROUP.value.type))  # NULL for wildcard
        .when(col("fuel_type") == "FUEL_TYPE_GASOLINE", lit(1).cast(ColumnType.FUEL_GROUP.value.type))
        .when(col("fuel_type") == "FUEL_TYPE_DIESEL", lit(2).cast(ColumnType.FUEL_GROUP.value.type))
        .otherwise(lit(0).cast(ColumnType.FUEL_GROUP.value.type)) # TODO map other fuel types, UNKNOWN for now
    ).withColumn(
        ColumnType.INTERNAL_SCALING.value.name,
        lit(None).cast(ColumnType.INTERNAL_SCALING.value.type)
    ).withColumn(
        ColumnType.UNIT.value.name,
        lit(None).cast(ColumnType.UNIT.value.type)
    ).withColumn(
        COMMENT_COLUMN.name,
        lit(None).cast(COMMENT_COLUMN.type)
    ).withColumn(
        ColumnType.DATA_SOURCE_ID.value.name,
        lit(SignalSourceType.GLOBAL.value).cast(ColumnType.DATA_SOURCE_ID.value.type)
    )

    # Add missing columns to j1979_df and prepend description to comment column before dropping description
    # so that it can be joined with passenger_standard_emitters_df.
    j1979_df = j1979_df.withColumn(ColumnType.OBD_VALUE.value.name, lit(None).cast(ColumnType.OBD_VALUE.value.type)) \
                       .withColumn(ColumnType.ENDIAN.value.name, lit(2).cast(ColumnType.ENDIAN.value.type)) \
                       .withColumn(ColumnType.INTERNAL_SCALING.value.name, lit(None).cast(ColumnType.INTERNAL_SCALING.value.type)) \
                       .withColumn(
                           COMMENT_COLUMN.name,
                           when(col("description").isNotNull() & col(COMMENT_COLUMN.name).isNotNull(),
                                expr(f"CONCAT(description, '\\n\\n', {COMMENT_COLUMN.name})"))
                           .when(col("description").isNotNull() & col(COMMENT_COLUMN.name).isNull(),
                                 col("description"))
                           .otherwise(col(COMMENT_COLUMN.name))
                       ) \
                       .drop("description")

    # Load standard emitters, and add a null unit column to bring in line with j1979_df
    passenger_standard_emitters_df = spark.sql("SELECT * from definitions.passenger_standard_emitter_signals") \
                                        .withColumn(ColumnType.UNIT.value.name, lit(None).cast(ColumnType.UNIT.value.type))

    # Find J1979DA signals that already exist in the passenger standard emitters table.
    # When incorporating the standard emitter signals, we will replace matching existing J1979DA signals with the
    # standard emitter versions, which include the OBD_VALUE.
    replacement_df = j1979_df.alias('a').join(passenger_standard_emitters_df.alias('b'),
                                              on=['data_identifier', 'bit_start', 'bit_length'],
                                              how='inner').select('a.*')
    j1979_with_standard_emitters_df = j1979_df.subtract(replacement_df).unionByName(passenger_standard_emitters_df)

    # Add missing columns to j1979_with_standard_emitters_df to allow union with global_obd_expanded_df.
    j1979_with_standard_emitters_df = j1979_with_standard_emitters_df \
                       .withColumn(ColumnType.MAKE.value.name, lit(None).cast(ColumnType.MAKE.value.type)) \
                       .withColumn(ColumnType.MODEL.value.name, lit(None).cast(ColumnType.MODEL.value.type)) \
                       .withColumn(ColumnType.YEAR.value.name, lit(None).cast(ColumnType.YEAR.value.type)) \
                       .withColumn(ColumnType.ENGINE_MODEL.value.name, lit(None).cast(ColumnType.ENGINE_MODEL.value.type)) \
                       .withColumn(ColumnType.POWERTRAIN.value.name, lit(None).cast(ColumnType.POWERTRAIN.value.type)) \
                       .withColumn(ColumnType.FUEL_GROUP.value.name, lit(None).cast(ColumnType.FUEL_GROUP.value.type)) \
                       .withColumn(ColumnType.DATA_SOURCE_ID.value.name, lit(SignalSourceType.J1979_DA.value).cast(ColumnType.DATA_SOURCE_ID.value.type)) \
                       .withColumn(ColumnType.PASSIVE_RESPONSE_MASK.value.name, lit(None).cast(ColumnType.PASSIVE_RESPONSE_MASK.value.type)) \
                       .withColumn(ColumnType.RESPONSE_ID.value.name, lit(None).cast(ColumnType.RESPONSE_ID.value.type)) \
                       .withColumn(ColumnType.REQUEST_ID.value.name, lit(None).cast(ColumnType.REQUEST_ID.value.type)) \
                       .withColumn(ColumnType.TRIM.value.name, lit(None).cast(ColumnType.TRIM.value.type))

    context.log.info(f"Combined J1979 with standard emitters with {j1979_with_standard_emitters_df.count():,} rows")

    # Process global obd config signals
    # Join global_obd_df with dim_vehicle_properties against MMYEF, matching regex patterns for make/model
    dim_vehicle_properties_df = spark.sql("SELECT DISTINCT make, model, year, engine_model, powertrain, fuel_group, trim FROM product_analytics_staging.dim_device_vehicle_properties")
    global_obd_expanded_df = (
        global_obd_df.alias('g')
        .join(
            dim_vehicle_properties_df.alias('p'),
            expr("""
                (
                    (g.make = '.*') OR
                    (p.make IS NOT NULL AND lower(p.make) RLIKE lower(g.make))
                ) AND
                (
                    (g.model = '.*') OR
                    (p.model IS NOT NULL AND LOWER(p.model) RLIKE g.model)
                ) AND
                (g.year IS NULL OR p.year = g.year) AND
                (g.engine_model IS NULL OR (LOWER(p.engine_model) = LOWER(g.engine_model))) AND
                (
                    -- config builder only checks electrification type (powertrain) if engine_model is empty
                    (g.engine_model IS NOT NULL) OR
                    -- "ANY" electrification_type (NULL powertrain) acts as wildcard (matches any powertrain)
                    (g.powertrain IS NULL) OR
                    (p.powertrain <=> g.powertrain)
                ) AND
                (
                    -- FUEL_TYPE_UNKNOWN (NULL fuel_group) acts as wildcard (matches any fuel_group)
                    (g.fuel_group IS NULL) OR
                    p.fuel_group <=> g.fuel_group
                )
            """),
            how='inner'
        )
        .select(
            col('p.make'),
            col('p.model'),
            col('p.year'),
            when(col('p.engine_model') == '', lit(None)).otherwise(col('p.engine_model')).alias('engine_model'),
            col('p.powertrain'),
            col('p.fuel_group'),
            col('p.trim'),
            col('g.obd_value'),
            col('g.bit_start'),
            col('g.bit_length'),
            col('g.sign'),
            col('g.endian'),
            col('g.internal_scaling'),
            col('g.unit'),
            col('g.scale'),
            col('g.offset'),
            col('g.minimum'),
            col('g.maximum'),
            # When data is broadcast, the data_identifier is not valid
            when((col('g.data_identifier') == 0) & col('g.passive_response_mask').isNotNull(),
                 lit(None)).otherwise(col('g.data_identifier')).alias('data_identifier'),
            col('g.passive_response_mask'),
            col('g.response_id'),
            col('g.request_id'),
            col('g.comment'),
            col('g.data_source_id'),
        ).distinct()
    )

    context.log.info(f"Expanded global OBD config to {global_obd_expanded_df.count():,} rows with matched MMYEF combinations")

    # Add unique primary key with deterministic ordering
    context.log.info("Adding row_id with deterministic ordering")
    window_spec = Window.orderBy(
        col("make").asc_nulls_last(),
        col("model").asc_nulls_last(),
        col("year").asc_nulls_last(),
        col("data_identifier").asc_nulls_last(),
        col("bit_start").asc_nulls_last(),
        col("comment").asc_nulls_last(),
    )

    # Get column names dynamically from COLUMNS definition
    column_names = [col.name if type(col) == Column else col.value.name for col in COLUMNS]

    # Create combined passenger signals DataFrame from distinct values of both sources
    combined_passenger_signals_df = global_obd_expanded_df.distinct() \
        .unionByName(j1979_with_standard_emitters_df.distinct()).distinct() \
        .withColumn(ROW_ID_COLUMN_BASE.name, row_number().over(window_spec).cast(ROW_ID_COLUMN_BASE.type)) \
        .withColumn(MMYEF_ID.name, expr(MMYEF_HASH_EXPRESSION.strip())) \
        .withColumn(ColumnType.BIT_START.value.name, col(ColumnType.BIT_START.value.name).cast(ColumnType.BIT_START.value.type)) \
        .withColumn(ColumnType.BIT_LENGTH.value.name, col(ColumnType.BIT_LENGTH.value.name).cast(ColumnType.BIT_LENGTH.value.type)) \
        .select(*column_names)

    context.log.info(f"Combined passenger signals DataFrame created with {combined_passenger_signals_df.count():,} distinct rows")

    return combined_passenger_signals_df
