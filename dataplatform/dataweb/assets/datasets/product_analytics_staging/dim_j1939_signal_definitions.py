"""
dim_j1939_signal_definitions

Enhanced J1939 signal definitions with VDP protocol compatibility and OBD mappings.
Loads J1939 signals from CSV source, applies VDP format transformations, and adds
OBD value mappings for comprehensive signal reference data.
"""

from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.utils import build_table_description, get_regional_firmware_s3_bucket
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
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
from dataweb.userpkgs.firmware.can.can_recompiler import SignType
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import regexp_replace, col, lit, when

COLUMNS = [
    Column(
        name="pgn",
        type=DataType.INTEGER,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Parameter Group Number (PGN) - J1939 message identifier."
        ),
    ),
    Column(
        name="spn",
        type=DataType.INTEGER,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Suspect Parameter Number (SPN) - J1939 signal identifier."
        ),
    ),
    ColumnType.BIT_START,
    ColumnType.BIT_LENGTH,
    ColumnType.ENDIAN,
    ColumnType.SIGN,
    ColumnType.OFFSET,
    ColumnType.SCALE,
    ColumnType.INTERNAL_SCALING,
    ColumnType.MINIMUM,
    ColumnType.MAXIMUM,
    ColumnType.UNIT,
    Column(
        name="pgn_label",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Short label for the Parameter Group Number."),
    ),
    Column(
        name="pgn_acronym",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Acronym for the Parameter Group Number."),
    ),
    Column(
        name="pgn_description",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Description of the Parameter Group Number."),
    ),
    Column(
        name="spn_label",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Short label for the Suspect Parameter Number."),
    ),
    Column(
        name="spn_description",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Description of the Suspect Parameter Number."),
    ),
    Column(
        name="mapping",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Additional mapping information."),
    ),
    ColumnType.OBD_VALUE,
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Enhanced J1939 signal definitions with VDP protocol compatibility and OBD mappings. "
        "Loads authoritative J1939 signal definitions from CSV source, applies VDP format transformations "
        "using EndiannessType and SignType enums for consistent interpretation, and adds OBD value name lookups where available. "
        "Serves as comprehensive reference table for automotive diagnostics and telematics signal interpretation.",
        row_meaning="Each row represents a processed J1939 signal definition for a specific PGN/SPN combination "
        "with VDP-compatible formatting and optional OBD value mappings.",
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_J1939_SIGNAL_DEFINITIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def dim_j1939_signal_definitions(context: AssetExecutionContext) -> DataFrame:
    """
    Load and process J1939 signal definitions with VDP compatibility and OBD mappings.

    This asset:
    1. Loads the authoritative J1939 signal definition CSV file
    2. Cleans description fields and applies VDP format transformations
    3. Maps PGN/SPN combinations to OBD values where available
    4. Provides comprehensive signal reference data for automotive diagnostics
    """
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Get region-specific S3 bucket path
    s3_bucket = get_regional_firmware_s3_bucket(context)

    context.log.info("Loading J1939 signals from CSV source with VDP processing")

    # Load CSV data with proper options for multiline and escaped content
    df_csv = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("multiline", "true")
        .option("escapeQuotes", "true")
        .option("escape", '"')
        .csv(f"{s3_bucket}/firmware/vdp/j1939_signals_june_2025.csv")
    )

    context.log.info(f"Loaded {df_csv.count():,} rows from CSV")

    # Clean description fields
    df_cleaned = (
        df_csv
        .withColumn("pgn_description", regexp_replace("pgn_description", "_x000D_", "\r"))
        .withColumn("spn_description", regexp_replace("spn_description", "_x000D_", "\r"))
    )

    # Load PGN/SPN to OBD value mappings from CSV
    pgn_spn_mappings = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{s3_bucket}/firmware/vdp/pgn_spn_obdvalue.csv")
    )
    
    context.log.info(f"Loaded {pgn_spn_mappings.count():,} PGN/SPN to OBD value mappings")

    result = (
        df_cleaned
        # From vdp.proto Endian - should already have EndiannessType enum values
        .withColumn("endian", col("endian").cast("long"))
        # From vdp.proto Sign - map to align with SignType enum values
        .withColumn("sign", 
            when(col("sign") == 0, lit(SignType.UNSIGNED))
            .when(col("sign") == 1, lit(SignType.SIGNED))
            .otherwise(lit(SignType.INVALID))
            .cast("long")
        )
        .withColumnRenamed("factor", "scale")
        # TODO: missing internal scaling from source to internal units
        .withColumn("internal_scaling", lit(1).cast("double"))
    )

    # Get column names dynamically from COLUMNS definition
    column_names = [col.name if type(col) == Column else col.value.name for col in COLUMNS]
    
    result_with_obd = (
        result.join(pgn_spn_mappings, ["pgn", "spn"], "left")
        .select(*column_names)
        .withColumn("obd_value", col("obd_value").cast("long"))
    )

    context.log.info(
        f"Processed {result_with_obd.count():,} enhanced J1939 signal definitions"
    )

    return result_with_obd
