# Databricks notebook source
# MAGIC %md
# MAGIC # Export Location Data for Tile Generation
# MAGIC
# MAGIC This notebook exports location data (railroad crossings, stop signs, traffic lights)
# MAGIC from Databricks tables to S3 for tile generation.
# MAGIC
# MAGIC ## Usage
# MAGIC Run with parameters:
# MAGIC - `tile_version`: The tile version (e.g., "35")
# MAGIC - `dataset_version`: The dataset version (e.g., "1")
# MAGIC - `layer_type`: One of "railroad", "stop_sign", "traffic_light", or "all"

# COMMAND ----------

# MAGIC %pip install /Volumes/s3/dataplatform-deployed-artifacts/wheels/service_credentials-1.0.1-py3-none-any.whl

# COMMAND ----------

import csv
import io
import os
from typing import Optional

import boto3
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws
import service_credentials  # required for ssm cloud credentials

# COMMAND ----------

# DBTITLE 1,Configuration

# S3 Configuration
BUCKET = "samsara-safety-map-data-sources"
SERVICE_CREDENTIAL_NAME = "samsara-safety-map-data-sources-readwrite"

# CSV Schema - must match what batch_generate_firmware_tiles.py expects
CSV_COLUMNS = ["node_id", "latitude", "longitude", "directions", "way_ids"]

# Layer type to S3 path mapping
LAYER_TYPE_TO_S3_PREFIX = {
    "railroad": "maptiles/railroad-locations-csvs",
    "stop_sign": "maptiles/stop-location-csvs",
    "traffic_light": "maptiles/traffic-light-locations-csvs",
}

# Layer type to Databricks table mapping
LAYER_TYPE_TO_TABLE = {
    "railroad": "dojo.osm_railroad_crossings_processed_latest",
    "stop_sign": "dojo.stop_sign_intersections_processed_latest",
    "traffic_light": "dojo.traffic_light_intersections_processed_latest",
}

# COMMAND ----------

# DBTITLE 1,Get Widget Parameters

dbutils.widgets.text("tile_version", "9999", "Tile Version")
dbutils.widgets.text("dataset_version", "1", "Dataset Version")
dbutils.widgets.dropdown(
    "layer_type", "all", ["all", "railroad", "stop_sign", "traffic_light"], "Layer Type"
)

tile_version = dbutils.widgets.get("tile_version")
dataset_version = dbutils.widgets.get("dataset_version")
layer_type = dbutils.widgets.get("layer_type")

print(f"Parameters:")
print(f"  tile_version: {tile_version}")
print(f"  dataset_version: {dataset_version}")
print(f"  layer_type: {layer_type}")

# COMMAND ----------

# DBTITLE 1,S3 Client Setup

boto3_session = boto3.Session(
    botocore_session=dbutils.credentials.getServiceCredentialsProvider(
        SERVICE_CREDENTIAL_NAME
    )
)
s3_client = boto3_session.client("s3")
s3_resource = boto3_session.resource("s3")

# COMMAND ----------

# DBTITLE 1,Data Loading Functions


def load_location_data(layer_type: str) -> DataFrame:
    """
    Load location data from the Databricks table for the specified layer type.

    The table must have columns that can be mapped to:
    - node_id: Integer (OSM node ID)
    - latitude: Float
    - longitude: Float
    - directions: String of direction chars (n, e, s, w)
    - way_ids: Comma-separated integers
    """
    table_name = LAYER_TYPE_TO_TABLE.get(layer_type)
    if not table_name or table_name == "<TBD>":
        raise ValueError(
            f"Table name not configured for layer type '{layer_type}'. "
            f"Please update LAYER_TYPE_TO_TABLE with the actual table name."
        )

    print(f"Loading data from table: {table_name}")
    df = spark.table(table_name)

    # Ensure required columns exist
    required_cols = {"node_id", "latitude", "longitude", "directions", "way_ids"}
    existing_cols = set(df.columns)
    missing_cols = required_cols - existing_cols

    if missing_cols:
        raise ValueError(
            f"Table {table_name} is missing required columns: {missing_cols}. "
            f"Available columns: {existing_cols}"
        )

    # Select only the required columns in the correct order
    result_df = df.select(CSV_COLUMNS)

    row_count = result_df.count()
    print(f"Loaded {row_count:,} rows from {table_name}")

    return result_df


def validate_data(df: DataFrame, layer_type: str) -> bool:
    """
    Validate the loaded data before export.
    Returns True if validation passes, raises exception otherwise.
    """
    row_count = df.count()

    # Check for empty data
    if row_count == 0:
        raise ValueError(
            f"No data found for layer type '{layer_type}'. Aborting export."
        )

    # Check for null values in critical columns
    for col_name in ["node_id", "latitude", "longitude"]:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"Found {null_count:,} null values in column '{col_name}' for layer type '{layer_type}'. "
                f"Please clean the data before export."
            )

    # Check for reasonable lat/lng bounds
    out_of_bounds = df.filter(
        (col("latitude") < -90)
        | (col("latitude") > 90)
        | (col("longitude") < -180)
        | (col("longitude") > 180)
    ).count()

    if out_of_bounds > 0:
        raise ValueError(
            f"Found {out_of_bounds:,} rows with lat/lng out of bounds for layer type '{layer_type}'."
        )

    print(f"Validation passed for {layer_type}: {row_count:,} rows")
    return True


# COMMAND ----------

# DBTITLE 1,Export Functions


def make_s3_key(layer_type: str, tile_version: str, dataset_version: str) -> str:
    """Generate the S3 key for the processed CSV."""
    prefix = LAYER_TYPE_TO_S3_PREFIX.get(layer_type)
    if not prefix:
        raise ValueError(f"Unknown layer type: {layer_type}")
    return f"{prefix}/{tile_version}/{dataset_version}/processed.csv"


def export_to_s3(
    df: DataFrame, layer_type: str, tile_version: str, dataset_version: str
):
    """
    Export DataFrame to S3 as a CSV file.

    Uses a temporary local file to handle the export, following the pattern
    from utils_dataset_write.py.
    """
    s3_key = make_s3_key(layer_type, tile_version, dataset_version)
    print(f"Exporting to s3://{BUCKET}/{s3_key}")

    # Create temp directory if it doesn't exist
    os.makedirs(f"/tmp/location_export_{layer_type}/", exist_ok=True)
    local_csv_path = f"/tmp/location_export_{layer_type}/processed.csv"

    # Convert to pandas and write CSV
    # Using pandas for consistent CSV formatting
    pdf = df.toPandas()
    pdf.to_csv(local_csv_path, index=False, quoting=csv.QUOTE_MINIMAL)

    print(f"Written {len(pdf):,} rows to local file: {local_csv_path}")

    # Upload to S3
    s3_client.upload_file(
        local_csv_path,
        BUCKET,
        s3_key,
        {"ACL": "bucket-owner-full-control"},
    )

    print(f"Successfully uploaded to s3://{BUCKET}/{s3_key}")

    # Clean up local file
    os.remove(local_csv_path)

    return f"s3://{BUCKET}/{s3_key}"


# COMMAND ----------

# DBTITLE 1,Main Export Logic


def export_layer(layer_type: str, tile_version: str, dataset_version: str) -> dict:
    """
    Export a single layer type to S3.
    Returns a dict with export metadata.
    """
    print(f"\n{'='*60}")
    print(f"Processing layer: {layer_type}")
    print(f"{'='*60}")

    # Load data
    df = load_location_data(layer_type)

    # Validate data
    validate_data(df, layer_type)

    # Export to S3
    s3_path = export_to_s3(df, layer_type, tile_version, dataset_version)

    row_count = df.count()

    return {
        "layer_type": layer_type,
        "row_count": row_count,
        "s3_path": s3_path,
        "tile_version": tile_version,
        "dataset_version": dataset_version,
    }


# COMMAND ----------

# DBTITLE 1,Execute Export

results = []

if layer_type == "all":
    # Export all layer types
    for lt in ["railroad", "stop_sign", "traffic_light"]:
        try:
            result = export_layer(lt, tile_version, dataset_version)
            results.append(result)
        except Exception as e:
            print(f"ERROR exporting {lt}: {e}")
            raise
else:
    # Export single layer type
    result = export_layer(layer_type, tile_version, dataset_version)
    results.append(result)

# COMMAND ----------

# DBTITLE 1,Summary

print("\n" + "=" * 60)
print("EXPORT SUMMARY")
print("=" * 60)

for result in results:
    print(f"\nLayer: {result['layer_type']}")
    print(f"  Rows exported: {result['row_count']:,}")
    print(f"  S3 path: {result['s3_path']}")
    print(f"  Tile version: {result['tile_version']}")
    print(f"  Dataset version: {result['dataset_version']}")

print("\n" + "=" * 60)
print("Export completed successfully!")
print("=" * 60)

# Return results for downstream processing
dbutils.notebook.exit(str(results))
