# Databricks notebook source
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

import json
import time
import csv
import os
import sqlite3
import boto3
import io
import pandas as pd
from pyspark.sql.functions import col, lit, explode, udf, coalesce
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType

# COMMAND ----------

# Define parameter constants
ARG_TILE_VERSION = "tile_version"
ARG_DATASET_VERSION = "dataset_version"
ARG_DATASET_NAME = "dataset_name"
ARG_OSM_REGIONS_VERSION_MAP = "osm_regions_version_map"
ARG_USE_LEGACY_BACKEND_CSV = "use_legacy_backend_csv"

# COMMAND ----------

"""
This notebook takes in the following args:
* ARG_TILE_VERSION:
Map tile version for the dataset
"""
dbutils.widgets.text(ARG_TILE_VERSION, "")
tile_version = dbutils.widgets.get(ARG_TILE_VERSION)
print(f"{ARG_TILE_VERSION}: {tile_version}")

"""
* ARG_DATASET_VERSION:
Dataset version for delta table naming (defaults to "0")
"""
dbutils.widgets.text(ARG_DATASET_VERSION, "0", "Dataset Version")
dataset_version = dbutils.widgets.get(ARG_DATASET_VERSION)
print(f"{ARG_DATASET_VERSION}: {dataset_version}")

"""
* ARG_DATASET_NAME:
Dataset name for delta table naming
"""
dbutils.widgets.text(ARG_DATASET_NAME, "", "Dataset Name")
dataset_name = dbutils.widgets.get(ARG_DATASET_NAME)
print(f"{ARG_DATASET_NAME}: {dataset_name}")

"""
* ARG_OSM_REGIONS_VERSION_MAP:
JSON dict from region -> version_id
"""
dbutils.widgets.text(ARG_OSM_REGIONS_VERSION_MAP, "")
osm_regions_to_version = dbutils.widgets.get(ARG_OSM_REGIONS_VERSION_MAP)
if len(osm_regions_to_version) > 0:
    osm_regions_to_version = json.loads(osm_regions_to_version)
else:
    osm_regions_to_version = {}
print(f"{ARG_OSM_REGIONS_VERSION_MAP}: {osm_regions_to_version}")

"""
* ARG_USE_LEGACY_BACKEND_CSV:
Whether to use legacy backend CSV files
"""
dbutils.widgets.text(ARG_USE_LEGACY_BACKEND_CSV, "")
use_legacy_backend_csv = dbutils.widgets.get(ARG_USE_LEGACY_BACKEND_CSV)
use_legacy_backend_csv = use_legacy_backend_csv.lower() == "true"
print(f"{ARG_USE_LEGACY_BACKEND_CSV}: {use_legacy_backend_csv}")


# COMMAND ----------

# Validate required parameters
if len(tile_version) == 0:
    exit_notebook("tile_version must be specified")
if len(dataset_name) == 0:
    exit_notebook("dataset_name must be specified")
if len(osm_regions_to_version) == 0:
    exit_notebook("osm_regions_version_map must be specified")

# COMMAND ----------

print(f"Processing incremental tiles for:")
print(f"  Tile Version: {tile_version}")
print(f"  Dataset Version: {dataset_version}")
print(f"  Dataset Name: {dataset_name}")
print(f"  OSM Regions to Version: {osm_regions_to_version}")

# COMMAND ----------

# Get delta table name using DBX_TABLE.delta function
delta_table_name = DBX_TABLE.delta_table(dataset_name, tile_version, dataset_version)

print(f"Combined delta table name: {delta_table_name}")

# COMMAND ----------

# Check if delta table exists
delta_table_exists = DBX_TABLE.is_table_exists(delta_table_name)

print(f"Combined delta table exists: {delta_table_exists}")

# COMMAND ----------


def write_fw_tiles_sqlite(
    delta_table_name: str,
    osm_regions_to_version: dict,
    tile_version: str,
    dataset_name: str,
    dataset_version: str,
):
    """
    Process combined delta dataset and create final dataframe for tile generation.

    Args:
        delta_table_name: Name of the combined delta table
        osm_regions_to_version: Dictionary mapping regions to OSM versions
        tile_version: Tile version string
        dataset_name: Dataset name
        dataset_version: Dataset version string
    """

    # Get way tiles table for tile information
    way_tiles_table = DBX_TABLE.osm_ways_tiles(osm_regions_to_version, zoom_level=13)
    print(f"Using way tiles table: {way_tiles_table}")

    # Read combined delta table if it exists
    if not delta_table_exists:
        print("No delta table exists, exiting...")
        return

    print("Reading delta table...")
    delta_df = spark.table(delta_table_name)
    print(f"Delta table has {delta_df.count()} rows")

    # Join with way tiles to get tile information
    print("Joining with way tiles table...")
    way_tiles_df = spark.table(way_tiles_table)

    # Explode the tiles array to create one row per way_id-tile pair
    # Use column aliases to avoid ambiguity
    delta_df_aliased = delta_df.alias("delta")
    way_tiles_df_aliased = way_tiles_df.alias("way_tiles")

    final_df = (
        delta_df_aliased.join(
            way_tiles_df_aliased,
            delta_df_aliased["way_id"] == way_tiles_df_aliased["way_id"],
            "inner",
        )
        .withColumn("tile_coordinates", explode("way_tiles.tile_coordinates"))
        .withColumn("tile_x", col("tile_coordinates.tile_x"))
        .withColumn("tile_y", col("tile_coordinates.tile_y"))
        .select(
            "delta.way_id",
            "override_speed_limit_milliknots",
            "commercial_vehicle_speed_map",
            "tile_x",
            "tile_y",
            "country_code",
            "state_code",
            "source",
        )
    )

    print(f"Final dataset has {final_df.count()} rows after tile explosion")

    # Convert to pandas for CSV creation
    final_pandas_df = final_df.toPandas()

    # Create CSV and upload to S3 following persist_speed_limits_tilegen_csv pattern
    print("Creating CSV file...")

    # New setup to S3 upload, according to https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5689307/How+To+Access+Cloud+Services+in+Databricks+SQS+SSM+etc.#How-To-Use-Cloud-Credentials
    boto3_session = boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(
            "samsara-safety-map-data-sources-readwrite"
        )
    )
    s3_client = boto3_session.client("s3")
    bucket = "samsara-safety-map-data-sources"

    # /tmp/ is the temporary folder in DBX Volumes, it only persists in memory
    os.makedirs(
        f"/tmp/incremental_map_tiles/decoupled_speed_limits_{dataset_name}/",
        exist_ok=True,
    )
    local_csv_path = f"/tmp/incremental_map_tiles/decoupled_speed_limits_{dataset_name}/dataset_df.csv"

    final_pandas_df.to_csv(
        local_csv_path, index=False, quoting=csv.QUOTE_NONE, escapechar="\\"
    )

    # Upload CSV to S3, and name it as tilegen_combined.csv
    csv_s3_key = f"incremental_map_tiles/speed_limits/{dataset_name}/{tile_version}/{dataset_version}/fw_tile_data/speed_limits_combined.csv"
    # The current _multi_part_upload func doesn't work via UC cluster, but
    # given that the tilegen-combined.csv is just ~2.5GB with the speed limits
    # covering the entire world that exceed the S3 upload limit of 5G, so let's
    # start with a simple upload for now for decoupled speed limits datasets
    s3_client.upload_file(
        local_csv_path,
        bucket,
        csv_s3_key,
        {"ACL": "bucket-owner-full-control"},
    )
    print("finished uploading speed_limits_combined.csv")

    # Write to sqlite db that will be used in python tile-generation code
    local_sqlite_path = f"/tmp/incremental_map_tiles/decoupled_speed_limits_{dataset_name}/speed_limit_export.sqlite3"

    try:
        con = sqlite3.connect(local_sqlite_path)
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS speed_limits;")
        cur.execute(
            """
            CREATE TABLE speed_limits (
                way_id BIGINT NOT NULL,
                speed_limit_milliknots INTEGER,
                commercial_vehicle_speed_map TEXT,
                tile_x INTEGER NOT NULL,
                tile_y INTEGER NOT NULL,
                tile_z INTEGER NOT NULL, 
                country_code VARCHAR(8),
                state_code VARCHAR(2),
                source VARCHAR(10)
            );
            """
        )
        con.commit()
        print("✅ SQLite table created successfully")
    except Exception as e:
        print(f"❌ Error creating SQLite table: {e}")
        return

    CHUNKSIZE = 60000
    with open(local_csv_path) as csvfile:
        reader = csv.DictReader(csvfile, escapechar="\\")
        count = 0
        rows_to_write = []

        for row in reader:
            # Read tile_x and tile_y directly from separate columns
            tile_x = int(row["tile_x"])
            tile_y = int(row["tile_y"])
            tile_z = 13  # Default z coordinate from zoom level

            sqlite_row = (
                int(row["way_id"]),
                int(float(row["override_speed_limit_milliknots"]))
                if row["override_speed_limit_milliknots"] != ""
                else None,
                row["commercial_vehicle_speed_map"],  # Add commercial vehicle speed map
                tile_x,
                tile_y,
                tile_z,
                row["country_code"],
                row["state_code"],
                row["source"],
            )
            rows_to_write.append(sqlite_row)
            if count % CHUNKSIZE == 0:
                cur.executemany(
                    "INSERT INTO speed_limits (way_id, speed_limit_milliknots, commercial_vehicle_speed_map, tile_x, tile_y, tile_z, country_code, state_code, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    rows_to_write,
                )
                con.commit()
                rows_to_write = []
                print(f"completed {count}")
            count += 1

        print(f"insert the last batch of rows: {count % CHUNKSIZE}")
        if rows_to_write:
            cur.executemany(
                "INSERT INTO speed_limits (way_id, speed_limit_milliknots, commercial_vehicle_speed_map, tile_x, tile_y, tile_z, country_code, state_code, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                rows_to_write,
            )
            con.commit()

    # Sqlite inserts get exponentially slower when an index is present on the table as the number of rows becomes large.
    # Since our use-case is write-once read-only, we can add the index after inserting all rows.
    cur.execute("CREATE INDEX way_id_idx ON speed_limits(way_id)")
    cur.execute("CREATE INDEX tile_idx ON speed_limits(tile_x, tile_y, tile_z)")
    cur.close()
    print("start uploading sqlitedb")

    # Upload the sqlite to S3
    s3_client.upload_file(
        local_sqlite_path,
        bucket,
        f"incremental_map_tiles/speed_limits/{dataset_name}/{tile_version}/{dataset_version}/fw_tile_data/speed_limits_combined.sqlite",
        {"ACL": "bucket-owner-full-control"},
    )

    print("finished uploading sqlitedb")
    print("✅ Successfully created CSV and SQLite database for tile generation")


def write_backend_csv(
    delta_table_name: str, dataset_name: str, tile_version: str, dataset_version: str
):
    """
    Write backend CSV files following the pattern from persist_backend_csv and persist_backend_csl_csv.
    This will partition the data into multiple CSV files and upload them to S3.

    This function mirrors the DAG approach (write_prod_ml_cv_datasets.py):
    1. Read resolved table (not delta table) and filter by org_id (ML-CV winners only)
    2. Compare with deployed to find actual changes (additions/updates)
    3. Find ML-CV way_ids that need soft-deletion (in deployed but not ML-CV winners)
    4. Union soft-delete rows with delta

    Args:
        delta_table_name: Name of the delta table (unused, kept for interface compatibility)
        dataset_name: Dataset name for S3 path
        tile_version: Tile version string
        dataset_version: Dataset version string
    """
    dataset_org_id = SPEED_LIMIT_DATASET_NAME_TO_DB_ORG_ID[dataset_name]
    current_ms = int(round(time.time() * 1000))

    # UDF to map data_source string to numeric value using DATASET_NAME_TO_DATASOURCE
    def map_source_to_datasource(source):
        datasource = DATASET_NAME_TO_DATASOURCE.get(source)
        return datasource.value if datasource else None

    map_source_to_datasource_udf = udf(map_source_to_datasource, IntegerType())

    # Default data_source for this dataset (fallback when source is not in mapping)
    default_datasource = DATASET_NAME_TO_DATASOURCE[dataset_name].value

    # Find the latest incremental resolved table for the dataset
    resolved_table_name = find_latest_incremental_dataset_resolved_speed_limit_table(
        dataset_name
    )
    print(f"Using resolved table for backend CSV: {resolved_table_name}")

    # Read resolved table and prepare result dataframe (ML-CV winners with speed > 0)
    # This mirrors create_speed_limits_backend_csv_with_verified_speed_limit_source_delta from DAG
    result_df = (
        spark.table(resolved_table_name)
        .filter("max_speed_limit IS NOT NULL")
        .filter(col("org_id") == dataset_org_id)  # filters to ML-CV winners only
        .withColumnRenamed("osm_way_id", "way_id")
        .withColumn("version_id", lit(int(tile_version)))
        .withColumn("dataset_version", lit(int(dataset_version)))
        .withColumn(
            "override_speed_limit_milliknots",
            convert_speed_to_milliknots_udf("max_speed_limit"),
        )
        .filter("override_speed_limit_milliknots > 0")  # Exclude 0-value (soft-deletes)
        .withColumn("created_at_ms", lit(current_ms))
        .withColumn("user_id", lit(0))
        .withColumn(
            # Map string data_source to numeric value, fall back to default if not found
            "data_source",
            coalesce(
                map_source_to_datasource_udf(col("data_source")),
                lit(default_datasource),
            ),
        )
        .select(
            "org_id",
            "way_id",
            "version_id",
            "dataset_version",
            "override_speed_limit_milliknots",
            "created_at_ms",
            "user_id",
            "data_source",
            "proto",
        )
    )

    # Create deployed_speed_limits_fw_tiles view for comparison
    create_deployed_speed_limits_fw_tiles_with_version_cap(
        dataset_org_id,
        tile_version,
        dataset_version,
    )

    # Compare with deployed to find actual changes (additions/updates)
    joined_df = (
        result_df.alias("result")
        .join(
            spark.table("deployed_speed_limits_fw_tiles").alias("deployed"),
            col("result.way_id") == col("deployed.way_id"),
            how="left",
        )
        .where(
            "deployed.way_id IS NULL or (deployed.way_id IS NOT NULL AND (deployed.speed_limit_milliknots != result.override_speed_limit_milliknots OR deployed.proto IS DISTINCT FROM result.proto))"
        )
        .where("result.override_speed_limit_milliknots IS NOT NULL")
    )

    # Log the counts
    count_null_deployed = joined_df.filter("deployed.way_id IS NULL").count()
    count_updates = joined_df.filter("deployed.way_id IS NOT NULL").count()
    print(f"Number of wayids to add with a new speed limit: {count_null_deployed}")
    print(f"Number of wayids to update with a new speed limit: {count_updates}")

    result_df = joined_df.select("result.*")

    # Find ML-CV way_ids that need soft-deletion (in deployed but not ML-CV winners in resolved)
    # This catches way_ids that are no longer ML-CV winners (removed from dojo or lost max() to TomTom)
    resolved_ml_cv_winners = (
        spark.table(resolved_table_name)
        .filter(col("org_id") == dataset_org_id)
        .filter("max_speed_limit IS NOT NULL AND max_speed_limit != '0 mph'")
        .select(col("osm_way_id").alias("way_id"))
        .distinct()
    )

    # Filter by org_id instead of data_source, because both ML_CV (data_source=4)
    # and global_override (data_source=5) entries share org_id=-3
    soft_delete_way_ids = (
        spark.table("deployed_speed_limits_fw_tiles")
        .filter(
            f"org_id = {dataset_org_id}"
        )  # org_id = -3 for ML_CV and global_override
        .filter("speed_limit_milliknots > 0")  # Exclude already-soft-deleted entries
        .select("way_id")
        .distinct()
        .alias("deployed_ml_cv")
        .join(
            resolved_ml_cv_winners.alias("resolved"),
            col("deployed_ml_cv.way_id") == col("resolved.way_id"),
            how="leftanti",  # way_ids in deployed but NOT in resolved ML-CV winners
        )
    )

    count_soft_deletes = soft_delete_way_ids.count()
    print(f"Number of ML-CV way_ids to soft-delete: {count_soft_deletes}")

    if count_soft_deletes > 0:
        # Create soft-delete rows with speed_limit = 0
        soft_delete_df = (
            soft_delete_way_ids.withColumn("org_id", lit(dataset_org_id))
            .withColumn("version_id", lit(int(tile_version)))
            .withColumn("dataset_version", lit(int(dataset_version)))
            .withColumn("override_speed_limit_milliknots", lit(0))
            .withColumn("created_at_ms", lit(current_ms))
            .withColumn("user_id", lit(0))
            .withColumn(
                "data_source", lit(DATASET_NAME_TO_DATASOURCE[dataset_name].value)
            )
            .withColumn("proto", lit(None).cast("string"))
            .select(result_df.columns)
        )

        # Union soft-delete rows with delta
        result_df = result_df.union(soft_delete_df)

    print(f"Overall number of wayids to upsert to speedlimitsdb: {result_df.count()}")

    # Use legacy backend CSV folder if use_legacy_backend_csv is true
    if use_legacy_backend_csv:
        persist_speed_limits_backend_csv(
            result_df, dataset_name, tile_version, dataset_version
        )
        return

    # Convert to pandas for non-legacy path
    dataset_df = result_df.toPandas()
    print(f"Backend CSV processing: {len(dataset_df)} rows")

    # Partition DF into separate files (following persist_backend_csv pattern)
    MAX_ROWS = 200000
    list_of_dfs = [
        dataset_df.loc[i : i + MAX_ROWS - 1, :]
        for i in range(0, len(dataset_df), MAX_ROWS)
    ]

    # Generate timestamp for unique filenames
    now_ms = int(round(time.time() * 1000))

    # Setup S3 client following the pattern from utils_dataset_write
    boto3_session = boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(
            "samsara-safety-map-data-sources-readwrite"
        )
    )
    s3 = boto3_session.resource("s3")
    bucket = "samsara-safety-map-data-sources"

    print(f"Writing {len(list_of_dfs)} backend CSV files...")

    for i in range(0, len(list_of_dfs)):
        # Create S3 key following the specified directory structure
        key = f"incremental_map_tiles/speed_limits/{dataset_name}/{tile_version}/{dataset_version}/backend_csvs/{now_ms}-{i}.csv"

        # Create CSV content in memory
        stream = io.StringIO()
        list_of_dfs[i].to_csv(stream, index=False)

        # Upload to S3
        obj = s3.Object(bucket, key)
        obj.put(Body=stream.getvalue(), ACL="bucket-owner-full-control")

        print(f"Uploaded CSV file {i+1}/{len(list_of_dfs)}: {key}")

    print(f"✅ Successfully uploaded {len(list_of_dfs)} backend CSV files to S3")


# COMMAND ----------

# Execute the tile generation if the combined table exists
if delta_table_exists:
    write_fw_tiles_sqlite(
        delta_table_name,
        osm_regions_to_version,
        tile_version,
        dataset_name,
        dataset_version,
    )

    # Also write backend CSV files
    write_backend_csv(delta_table_name, dataset_name, tile_version, dataset_version)
else:
    print("❌ No delta table exists to process")


# COMMAND ----------

exit_notebook()

# COMMAND ----------
