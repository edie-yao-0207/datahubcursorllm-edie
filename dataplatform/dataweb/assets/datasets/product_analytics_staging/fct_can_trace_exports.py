"""
CAN Trace Exports Asset

This asset combines ASC chunks from fct_can_trace_asc_chunks into complete ASC files
and writes them to S3. It tracks export metadata including file paths and status.
"""

from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.constants import (
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    DQCheckMode,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
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
    ProductAnalyticsStaging,
    ProductAnalytics,
    DataModelCoreBronze,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import (
    create_run_config_overrides,
    format_date_partition_query,
)
from dataweb.userpkgs.utils import build_table_description

# Spark imports for dataweb framework
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.TRACE_UUID,
    Column(
        name="s3_path",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="S3 path where the complete ASC file is stored."),
    ),
    Column(
        name="name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Filename of the exported ASC file."),
    ),
    Column(
        name="file_size_bytes",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Total size of the exported ASC file in bytes."),
    ),
    Column(
        name="total_chunks",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(comment="Total number of chunks combined into this file."),
    ),
    Column(
        name="total_frames",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Total number of CAN frames in the exported file."),
    ),
    Column(
        name="metadata_json_path",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Path to the generated JSON metadata file."),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

# Export configuration
OUTPUT_PATH_BASE = "/Volumes/s3/firmware-test-automation/root/owltomation_test_data/can_files"
S3_BUCKET_BASE = "s3://samsara-firmware-test-automation/owltomation_test_data/can_files"

# Schema for pandas UDF output
EXPORT_SCHEMA = StructType(
    [
        StructField("trace_uuid", StringType(), False),
        StructField("s3_path", StringType(), False),
        StructField("name", StringType(), False),
        StructField("file_size_bytes", LongType(), False),
        StructField("total_chunks", IntegerType(), False),
        StructField("total_frames", LongType(), False),
        StructField("metadata_json_path", StringType(), True),
    ]
)

QUERY = """
WITH data AS (
    SELECT
        date,
        org_id,
        device_id,
        trace_uuid,
        part_idx,
        asc_text,
        num_frames
    FROM {product_analytics_staging}.fct_can_trace_asc_chunks
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
),

device_metadata AS (
    SELECT
        dim.date,
        dim.org_id,
        dim.device_id,
        dim.make,
        dim.model,
        dim.year,
        dim.engine_model,
        dim.product_name,
        dim.cable_name,
        vin.vin
    FROM product_analytics.dim_device_dimensions AS dim
    LEFT JOIN datamodel_core_bronze.raw_vindb_shards_device_vin_metadata AS vin USING (date, org_id, device_id)
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
)

SELECT 
    data.*,
    dm.make,
    dm.model,
    dm.year,
    dm.engine_model,
    dm.product_name,
    dm.cable_name,
    dm.vin
FROM data
LEFT JOIN device_metadata dm USING (date, org_id, device_id)
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="This table tracks CAN trace exports to ASC files with metadata. Each row represents "
        "a complete ASC file and JSON metadata file that have been generated from ASC chunks and written to S3. "
        "The ASC files are compatible with Vector CANoe and other automotive analysis tools, while the "
        "metadata files provide vehicle and device context information.",
        row_meaning="Each row represents a complete ASC file export with metadata for a specific CAN trace.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_CAN_TRACE_ASC_CHUNKS),
        AnyUpstream(ProductAnalytics.DIM_DEVICE_DIMENSIONS),
        AnyUpstream(DataModelCoreBronze.RAW_VINDB_SHARDS_DEVICE_VIN_METADATA),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=8,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CAN_TRACE_EXPORTS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_can_trace_exports(context: AssetExecutionContext) -> DataFrame:
    """
    Combine ASC chunks into complete files and export to S3 with metadata.

    This asset takes ASC chunks from fct_can_trace_asc_chunks, combines them in order,
    and writes complete ASC files to S3 along with JSON metadata files containing
    vehicle and device information. It tracks export metadata including file paths,
    sizes, chunk counts, and vehicle specifications.

    Processing strategy:
    1. Query new ASC chunks with device metadata (traces not already exported)
    2. Group chunks by trace_uuid and combine in order
    3. Write complete ASC files to S3
    4. Write JSON metadata files with vehicle/device information
    5. Record export metadata with vehicle specifications

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Spark DataFrame containing export metadata for the current partition
    """
    # Initialize Spark session
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Helper function to return empty Spark DataFrame with proper schema
    def get_empty_dataframe():
        return spark.createDataFrame([], EXPORT_SCHEMA)

    # Get ASC chunks for processing
    formatted_query = format_date_partition_query(QUERY, context)
    context.log.info(f"Executing query to get ASC chunks:\n{formatted_query}")

    df = spark.sql(formatted_query)

    # Define pandas UDF for writing ASC files and metadata
    def write_asc_trace_and_metadata(pdf):
        """
        Combine ASC chunks, write complete ASC file, and generate metadata JSON.

        Args:
            pdf: pandas DataFrame with columns:
                - trace_uuid: String UUID of the trace
                - part_idx: Integer chunk index
                - asc_text: String ASC text content
                - num_frames: Integer number of frames in chunk
                - make, model, year, etc.: Vehicle metadata

        Returns:
            pandas DataFrame with export metadata
        """
        import pandas as pd
        import os
        import io
        import json
        import numpy as np

        def convert_np(obj):
            """Recursively convert NumPy types to native Python types."""
            if isinstance(obj, np.generic):
                return obj.item()
            elif isinstance(obj, dict):
                return {k: convert_np(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_np(i) for i in obj]
            else:
                return obj

        if pdf.empty:
            return pd.DataFrame(
                columns=[
                    "trace_uuid",
                    "s3_path",
                    "name",
                    "file_size_bytes",
                    "total_chunks",
                    "total_frames",
                    "metadata_json_path",
                ]
            )

        trace_uuid = pdf["trace_uuid"].iloc[0]
        df_sorted = pdf.sort_values("part_idx")

        # Create output file paths
        asc_file_name = f"{trace_uuid}.asc"
        asc_file_path = f"{OUTPUT_PATH_BASE}/{asc_file_name}"
        metadata_file_name = f"{trace_uuid}.meta.json"
        metadata_file_path = f"{OUTPUT_PATH_BASE}/{metadata_file_name}"

        # Combine all ASC text chunks
        out = io.StringIO()
        total_frames = 0
        for _, row in df_sorted.iterrows():
            out.write(row["asc_text"])
            total_frames += row["num_frames"]

        complete_asc = out.getvalue()
        file_size_bytes = len(complete_asc.encode("utf-8"))

        # Ensure directory exists
        os.makedirs(os.path.dirname(asc_file_path), exist_ok=True)

        # Write ASC file
        with open(asc_file_path, "w") as f:
            f.write(complete_asc)

        # Generate comprehensive metadata dictionary
        first_row = pdf.iloc[0]
        metadata = {
            "trace_uuid": trace_uuid,
            "org_id": str(first_row.get("org_id", "")),
            "device_id": str(first_row.get("device_id", "")),
            "s3_path": f"{S3_BUCKET_BASE}/{asc_file_name}",
            "name": asc_file_name,  # File name for DataFrame
            "make": str(first_row.get("make", ""))
            if pd.notna(first_row.get("make"))
            else None,
            "model": str(first_row.get("model", ""))
            if pd.notna(first_row.get("model"))
            else None,
            "year": str(first_row.get("year", ""))
            if pd.notna(first_row.get("year"))
            else None,
            "engine_model": str(first_row.get("engine_model", ""))
            if pd.notna(first_row.get("engine_model"))
            else None,
            "product_name": str(first_row.get("product_name", ""))
            if pd.notna(first_row.get("product_name"))
            else None,
            "cable_name": str(first_row.get("cable_name", ""))
            if pd.notna(first_row.get("cable_name"))
            else None,
            "vin": str(first_row.get("vin", ""))
            if pd.notna(first_row.get("vin"))
            else None,
            "date": str(first_row.get("date", "")),
            "total_frames": total_frames,
            "total_chunks": len(df_sorted),
            "file_size_bytes": file_size_bytes,
            "metadata_json_path": f"{S3_BUCKET_BASE}/{metadata_file_name}",  # Add S3 path to metadata
        }

        # Clean up numpy types and write metadata JSON
        metadata_cleaned = convert_np(metadata)
        try:
            with open(metadata_file_path, "w") as f:
                json.dump(metadata_cleaned, f, indent=2)
        except Exception as e:
            print(f"Warning: Failed to write metadata JSON for {trace_uuid}: {e}")
            # If JSON write fails, clear the metadata_json_path
            metadata["metadata_json_path"] = None

        # Prepare DataFrame record using metadata as single source of truth
        dataframe_record = {
            "trace_uuid": metadata["trace_uuid"],
            "s3_path": metadata["s3_path"],
            "name": metadata["name"],
            "file_size_bytes": metadata["file_size_bytes"],
            "total_chunks": metadata["total_chunks"],
            "total_frames": metadata["total_frames"],
            "metadata_json_path": metadata["metadata_json_path"],
        }

        return pd.DataFrame([dataframe_record])

    # Process traces and write files
    context.log.info("Combining ASC chunks and writing files with metadata to S3")

    export_result = df.groupBy("trace_uuid").applyInPandas(
        write_asc_trace_and_metadata, schema=EXPORT_SCHEMA
    )

    # Add back metadata columns for partitioning
    trace_metadata = df.select("trace_uuid", "date", "org_id", "device_id").distinct()

    final_result = export_result.join(
        trace_metadata, on="trace_uuid", how="inner"
    ).select(
        "date",
        "org_id",
        "device_id",
        "trace_uuid",
        "s3_path",
        "name",
        col("file_size_bytes").cast("long").alias("file_size_bytes"),
        col("total_chunks").cast("int").alias("total_chunks"),
        col("total_frames").cast("long").alias("total_frames"),
        "metadata_json_path",
    )

    export_count = final_result.count()
    context.log.info(f"Successfully exported {export_count:,} ASC files")

    return final_result
