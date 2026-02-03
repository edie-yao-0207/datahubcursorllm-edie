"""
dim_device_vehicle_properties

Daily snapshot of vehicle properties by org/device for measuring population sizes.
Combines device dimensions with VIN metadata and fuel properties to provide 
comprehensive vehicle characteristics used for population analysis and segmentation.
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr
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
    DataType,
    Metadata,
    ColumnType,
    columns_to_schema,
    get_column_names,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    ProductAnalytics,
    DatamodelCoreBronze,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

# Common MMYEF_ID hash expression
# Coalesces are required for hashes to be unique
# FIXED: Normalized NULL vs empty string handling using NULLIF(TRIM(field), "")
# to ensure consistent hashing between dimension tables and signal sources
MMYEF_HASH_EXPRESSION = """
XXHASH64(
    COALESCE(make, "INVALID"),
    COALESCE(model, "INVALID"),
    COALESCE(year, -100),  -- Use -100 for NULL year to distinguish from actual 0
    UPPER(COALESCE(NULLIF(TRIM(engine_model), ""), "INVALID")),  -- Normalize empty strings to NULL, then to INVALID
    COALESCE(fuel_group, -100),  -- Use -100 for NULL fuel_group to distinguish from actual 0
    COALESCE(powertrain, -100),  -- Use -100 for NULL powertrain to distinguish from actual 0
    UPPER(COALESCE(NULLIF(TRIM(trim), ""), "INVALID"))  -- Normalize empty strings to NULL, then to INVALID
)
"""

MMYEF_ID = Column(
    name="mmyef_id",
    type=DataType.LONG,
    nullable=True,
    metadata=Metadata(
        comment="XXHASH64 of vehicle characteristics (Make, Model, Year, Engine, Fuel) for efficient joining with signal mappings and vehicle-specific processing."
    ),
)

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
    ColumnType.POWERTRAIN,
    ColumnType.FUEL_GROUP,
    ColumnType.TRIM,
    MMYEF_ID,
]

PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)
SCHEMA = columns_to_schema(*COLUMNS)
COLUMN_NAMES = get_column_names(COLUMNS)

QUERY = """
SELECT
    date,
    org_id,
    device_id,
    dim.make,
    dim.model,
    dim.year,
    UPPER(dim.engine_model) AS engine_model,
    fuel.fuel_group,
    fuel.powertrain,
    UPPER(CASE
        WHEN vin.trim IS NULL AND vin.trim_secondary IS NULL THEN null
        ELSE CONCAT_WS(" ", vin.trim, vin.trim_secondary)
    END) AS trim

FROM product_analytics.dim_device_dimensions AS dim
LEFT JOIN datamodel_core_bronze.raw_vindb_shards_device_vin_metadata AS vin USING (date, org_id, device_id)
LEFT JOIN {product_analytics_staging}.stg_daily_fuel_properties AS fuel USING (date, org_id, device_id)

WHERE date BETWEEN "{date_start}" AND "{date_end}"
  AND dim.product_name LIKE "VG%"
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily snapshot of vehicle properties by org/device for population measurement. "
        "Combines device dimensions with VIN metadata and fuel properties to provide comprehensive "
        "vehicle characteristics used for population analysis and segmentation. Each row represents "
        "a unique device with its complete vehicle property profile on the snapshot date.",
        row_meaning="Each row represents a single device with its complete vehicle property profile "
        "including make, model, year, engine, fuel, and trim information for population analysis.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalytics.DIM_DEVICE_DIMENSIONS),
        AnyUpstream(DatamodelCoreBronze.RAW_VINDB_SHARDS_DEVICE_VIN_METADATA),
        AnyUpstream(ProductAnalyticsStaging.STG_DAILY_FUEL_PROPERTIES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def dim_device_vehicle_properties(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    formatted_query = format_date_partition_query(QUERY, context)
    context.log.info(formatted_query)

    df = spark.sql(formatted_query)

    # Apply the standardized MMYEF_ID hash expression
    df = df.withColumn("mmyef_id", expr(MMYEF_HASH_EXPRESSION.strip()))

    # Select only the columns defined in the schema to avoid schema mismatch
    return df.select(*COLUMN_NAMES)
