# Databricks notebook source
# MAGIC %pip install overpy
# MAGIC %pip install h3
# MAGIC %pip install shapely
# MAGIC %pip install geopandas
# MAGIC %pip install networkx

# COMMAND ----------

# MAGIC %run backend/safety/low_bridge_strikes/here_maps_processor

# COMMAND ----------

# MAGIC %run backend/safety/low_bridge_strikes/udf_helpers

# COMMAND ----------

# MAGIC %run backend/safety/low_bridge_strikes/dangerous_ways_processor

# COMMAND ----------

# MAGIC %run backend/safety/low_bridge_strikes/osm_here_maps_matcher

# COMMAND ----------

# MAGIC %run backend/safety/low_bridge_strikes/osm_processor

# COMMAND ----------

# MAGIC %run backend/safety/low_bridge_strikes/bridge_group_manager

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import time
import json

import boto3
from boto3.s3.transfer import TransferConfig
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import uuid
import os

# Standard arguments and values
ARG_REGIONS = "regions"  # Supports multiple regions separated by commas
ARG_TILE_VERSION = "tile_version"
ARG_OSM_VERSION = "osm_dataset_version"
ARG_BRIDGE_DATASET_VERSION = "bridge_dataset_version"
ARG_FETCH_HERE_MAPS = "fetch_here_maps"
ARG_TEST = "test"
ARG_OSM_REGIONS_VERSION_MAP = "osm_regions_versions"


def generate_table_name_with_date(base_name: str):
    today = time.strftime("%Y_%m_%d")
    return f"{base_name}_{today}"


def create_spark_session():
    return SparkSession.builder.appName("LBS").getOrCreate()


def region_full_name(region: str):
    return {
        "EUR": "Europe",
        "USA": "United States of America",
        "MEX": "Mexico",
        "CAN": "Canada",
        "SF": "San Francisco",
    }[region]


def get_osm_table_name(region: str, date: str = "20240325"):
    return f"safety_map_data.osm_{date}__{region.lower()}__full_ways"


def str_to_bool(value):
    return value.lower() == "true"


def get_secret_value(keyname: str, region):
    secrets_client = boto3.client("secretsmanager", region_name=region)
    try:
        res = secrets_client.get_secret_value(SecretId=keyname)
    except:
        return {"api_key": ""}
    return json.loads(res["SecretString"])


def upload_file_to_s3(tile_version: str, dataset_version: str, filename, df: DataFrame):
    s3_bucket = "safety-map-data-sources"
    s3_key = f"maptiles/bridge-locations/{tile_version}/{dataset_version}"

    temp_dir = f"/Volumes/s3/{s3_bucket}/{s3_key}/tmp/{uuid.uuid4().hex}"

    df.coalesce(1).write.format("csv").mode("overwrite").option("header", True).save(
        temp_dir
    )

    csv_files = [f for f in os.listdir(temp_dir) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError(f"No CSV file found in {temp_dir}")

    generated_file = os.path.join(temp_dir, csv_files[0])

    # Rename the file by copying it to the destination.
    with open(generated_file, "rb") as src, open(
        f"/Volumes/s3/{s3_bucket}/{s3_key}/{filename}.csv", "wb"
    ) as dst:
        while True:
            chunk = src.read(10 * 1024 * 1024)  # 10 MB chunks
            if not chunk:
                break
            dst.write(chunk)


dbutils.widgets.text(
    ARG_REGIONS, "EUR", "Regions (comma-separated, e.g., EUR,USA,MEX,CAN)"
)
dbutils.widgets.text(ARG_TILE_VERSION, "9999", "Tile Version")
dbutils.widgets.text(
    ARG_OSM_VERSION, "", "OSM Dataset Version"
)  # Won't be specified for quarterly map tile refreshes.
dbutils.widgets.text(ARG_BRIDGE_DATASET_VERSION, "9999", "Bridge Dataset Version")
dbutils.widgets.dropdown(
    ARG_FETCH_HERE_MAPS,
    "false",
    ["true", "false"],
    "Fetch data from HERE maps API (Note: This may " "generate additional costs).",
)
dbutils.widgets.dropdown(ARG_TEST, "false", ["true", "false"], "Test Mode")

"""
Process bridge location data for multiple regions.
Regions are specified as a comma-separated list (e.g., "EUR,USA,MEX,CAN").
For each region, the notebook:
1. Processes OSM data and optionally HERE Maps data
2. Identifies bridge connections
3. Generates bridge metadata, lookup entries, and node data

After processing all regions, the datasets are unioned and written to S3.
"""
regions = [r.strip() for r in dbutils.widgets.get(ARG_REGIONS).split(",")]
print(f"Processing regions: {regions}")

# ARG_OSM_REGIONS_VERSION_MAP is used for quarterly map tile refreshes.
dbutils.widgets.text(ARG_OSM_REGIONS_VERSION_MAP, "")
osm_regions_to_version = dbutils.widgets.get(ARG_OSM_REGIONS_VERSION_MAP)
if len(osm_regions_to_version) > 0:
    osm_regions_to_version = json.loads(osm_regions_to_version)
else:
    osm_regions_to_version = {}
print(f"{ARG_OSM_REGIONS_VERSION_MAP}: {osm_regions_to_version}")

# Common parameters (not region-specific)
common_params = {
    "api_key_here_maps": get_secret_value("here-maps-api-key", region="us-west-2")[
        "api_key"
    ],
    "fetch_here_maps": str_to_bool(dbutils.widgets.get(ARG_FETCH_HERE_MAPS)),
    "test": str_to_bool(dbutils.widgets.get(ARG_TEST)),
    "table_lbs_nodes": generate_table_name_with_date("lbs_nodes"),
    "bridge_dataset_version": dbutils.widgets.get(ARG_BRIDGE_DATASET_VERSION),
    "bridge_metadata_entries": generate_table_name_with_date("bridge_metadata_entries"),
    "node_entries": generate_table_name_with_date("node_entries"),
    "bridge_lookup_entries": generate_table_name_with_date("bridge_lookup_entries"),
    "result_database_name": "dataprep_routing",
    "tile_version": dbutils.widgets.get(ARG_TILE_VERSION),
}

# Get default OSM version if not in mapping
default_osm_dataset_version = dbutils.widgets.get(ARG_OSM_VERSION)

spark = create_spark_session()

# Lists to collect datasets from each region
all_bridge_metadata = []
all_connections_dataset = []
all_connections_nodes = []

# Process each region
for region in regions:
    print(f"\n{'='*80}")
    print(f"Processing region: {region}")
    print(f"{'='*80}")

    # Determine OSM dataset version for this region
    if osm_regions_to_version != {}:
        if region not in osm_regions_to_version:
            print(
                f"Warning: Region {region} not found in osm_regions_to_version, skipping..."
            )
            continue
        osm_dataset_version = osm_regions_to_version[region]
    else:
        if default_osm_dataset_version == "":
            print(f"Error: OSM dataset version not specified for region {region}")
            continue
        osm_dataset_version = default_osm_dataset_version

    print(f"Using OSM dataset version: {osm_dataset_version} for region {region}")

    # Create region-specific params
    params = {
        **common_params,
        "region": region_full_name(region),
        "osm_table_name": get_osm_table_name(region, osm_dataset_version),
    }

    # Process OSM data for this region
    osm_processor = OpenStreetMapDataProcessor(spark, params)

    # Process HERE Maps data if enabled
    if params["fetch_here_maps"]:
        here_maps_data = HereMapAttributes(
            api_key=params["api_key_here_maps"],
            region=params["region"],
        ).road_data
        df_here_maps_spark = spark.createDataFrame(here_maps_data)
        df_here_maps_spark = df_here_maps_spark.withColumn(
            "wkt", wkt_udf(col("LAT"), col("LON"))
        )
        df_here_maps_spark = (
            df_here_maps_spark.withColumnRenamed("HEIGHT_RESTRICTION", "max_height")
            .withColumnRenamed("LINK_IDS", "link_id")
            .select("link_id", "max_height", "wkt")
        )

        matcher = DataFrameMatcher(osm_processor.df_osm_maps_spark, df_here_maps_spark)
        df_all_roads = matcher.df_all_roads
    else:
        df_all_roads = osm_processor.df_osm_maps_spark.select(
            col("way_id"),
            col("nodes"),
            col("max_height"),
            col("coordinates"),
            col("exit_node"),
        )

    # BridgeGroupManager groups connected roads by their max_height attribute.
    # This prevents the same bridge from being counted multiple times.
    bridge_group_manager = BridgeGroupManager(spark, df_all_roads)
    df_all_roads = bridge_group_manager.create_bridge_groups()

    # Process dangerous ways (bridge connections)
    processor = DangerousWaysProcessor(
        spark, df_all_roads, osm_processor.df_nodes, osm_processor.df_nodes_lat_lng
    )
    processor.process_data()

    # Extract datasets for this region
    connections_dataset_ordered = processor.connections_dataset_ordered

    bridge_metadata = (
        df_all_roads.filter(col("max_height").isNotNull())
        .select("way_id", "max_height", "bridge_group_id")
        .withColumnRenamed("max_height", "bridge_height_meters")
    )

    connections_nodes = processor.get_connections_nodes()

    # Collect datasets for union later
    all_bridge_metadata.append(bridge_metadata)
    all_connections_dataset.append(connections_dataset_ordered)
    all_connections_nodes.append(connections_nodes)

    print(f"Completed processing region {region}")
    print(f"  - Bridge metadata rows: {bridge_metadata.count()}")
    print(f"  - Bridge lookup entries: {connections_dataset_ordered.count()}")
    print(f"  - Connection nodes: {connections_nodes.count()}")

# Union all regional datasets
print(f"\n{'='*80}")
print("Unioning datasets from all regions")
print(f"{'='*80}")

if len(all_bridge_metadata) == 0:
    print("Error: No regions were successfully processed")
    raise Exception("No regions were successfully processed")

# Union bridge metadata
final_bridge_metadata = all_bridge_metadata[0]
for df in all_bridge_metadata[1:]:
    final_bridge_metadata = final_bridge_metadata.unionByName(df)
print(f"Final bridge metadata rows: {final_bridge_metadata.count()}")

# Union bridge lookup entries
final_connections_dataset = all_connections_dataset[0]
for df in all_connections_dataset[1:]:
    final_connections_dataset = final_connections_dataset.unionByName(df)
print(f"Final bridge lookup entries: {final_connections_dataset.count()}")

# Union connection nodes
final_connections_nodes = all_connections_nodes[0]
for df in all_connections_nodes[1:]:
    final_connections_nodes = final_connections_nodes.unionByName(df)
print(f"Final connection nodes: {final_connections_nodes.count()}")

# Write final datasets to tables and S3
if not common_params["test"]:
    print(f"\n{'='*80}")
    print("Writing final datasets")
    print(f"{'='*80}")

    # Write bridge metadata
    bridge_metadata_cached = final_bridge_metadata.cache()
    bridge_metadata_cached.write.mode("overwrite").option(
        "overwriteSchema", "True"
    ).saveAsTable(
        f'{common_params["result_database_name"]}.{common_params["bridge_metadata_entries"]}'
    )
    upload_file_to_s3(
        common_params["tile_version"],
        common_params["bridge_dataset_version"],
        "bridge_metadata",
        bridge_metadata_cached,
    )
    bridge_metadata_cached.unpersist()
    print("✓ Bridge metadata written")

    # Write bridge lookup entries
    connections_dataset_cached = final_connections_dataset.cache()
    connections_dataset_cached.write.mode("overwrite").option(
        "overwriteSchema", "True"
    ).saveAsTable(
        f'{common_params["result_database_name"]}.{common_params["bridge_lookup_entries"]}'
    )
    upload_file_to_s3(
        common_params["tile_version"],
        common_params["bridge_dataset_version"],
        "bridge_lookup_entries",
        connections_dataset_cached,
    )
    connections_dataset_cached.unpersist()
    print("✓ Bridge lookup entries written")

    # Write connection nodes
    connections_nodes_cached = final_connections_nodes.cache()
    connections_nodes_cached.write.mode("overwrite").option(
        "overwriteSchema", "True"
    ).saveAsTable(
        f'{common_params["result_database_name"]}.{common_params["table_lbs_nodes"]}'
    )
    upload_file_to_s3(
        common_params["tile_version"],
        common_params["bridge_dataset_version"],
        "lbs_nodes",
        connections_nodes_cached,
    )
    connections_nodes_cached.unpersist()
    print("✓ Connection nodes written")

    print(f"\n{'='*80}")
    print("All datasets successfully written to S3")
    print(f"{'='*80}")
else:
    print("\nTest mode: Showing sample data")
    print("\nBridge Metadata:")
    final_bridge_metadata.show(100)
    print("\nBridge Lookup Entries:")
    final_connections_dataset.show(100)
    print("\nConnection Nodes:")
    final_connections_nodes.show(100)
