# COMMAND ----------

# MAGIC %pip install mercantile

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

import json
import math

import mercantile
from pyspark.sql.functions import (
    array_distinct,
    col,
    collect_list,
    explode,
    lit,
    struct,
    udf,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Standard arguments and values
ARG_OSM_REGIONS_VERSION_MAP = "osm_regions_versions"
ARG_ZOOM_LEVEL = "zoom_level"

"""
This notebook takes in the following args:
* ARG_OSM_REGIONS_VERSION_MAP:
JSON dict from region -> version_id
* ARG_ZOOM_LEVEL:
Zoom level for slippy tile calculations (default: 13)
"""
dbutils.widgets.text(ARG_OSM_REGIONS_VERSION_MAP, "")
dbutils.widgets.text(ARG_ZOOM_LEVEL, "13")

osm_regions_to_version = dbutils.widgets.get(ARG_OSM_REGIONS_VERSION_MAP)
zoom_level = int(dbutils.widgets.get(ARG_ZOOM_LEVEL))

if len(osm_regions_to_version) > 0:
    osm_regions_to_version = json.loads(osm_regions_to_version)
else:
    osm_regions_to_version = {}

print(f"{ARG_OSM_REGIONS_VERSION_MAP}: {osm_regions_to_version}")
print(f"{ARG_ZOOM_LEVEL}: {zoom_level}")

# COMMAND ----------

# Iterate over osm_regions_to_version map and find full ways table names
print("Finding OSM full ways table names for each region:")
for region, version_id in osm_regions_to_version.items():
    full_ways_table_name = DBX_TABLE.osm_full_ways(version_id, region)
    print(f"Region: {region}, Version: {version_id}, Table: {full_ways_table_name}")

    # Check if table exists
    if DBX_TABLE.is_table_exists(full_ways_table_name):
        print(f"  ✓ Table exists")
    else:
        print(f"  ✗ Table does not exist")

# COMMAND ----------


# COMMAND ----------


def lat_lng_to_slippy_tile(lat, lng, zoom=13):
    """
    Convert lat/lng to slippy tile coordinates using mercantile library.

    Args:
        lat: Latitude in degrees (-90 to 90)
        lng: Longitude in degrees (-180 to 180)
        zoom: Zoom level (0 to 20, default 13)

    Returns:
        tuple: (tile_x, tile_y) or None if inputs are invalid
    """
    # Handle null/None values
    if lat is None or lng is None or zoom is None:
        return None

    # Handle NaN values
    if math.isnan(lat) or math.isnan(lng) or math.isnan(zoom):
        return None

    # Validate latitude range (-90 to 90)
    if lat < -90 or lat > 90:
        return None

    # Validate longitude range (-180 to 180)
    if lng < -180 or lng > 180:
        return None

    # Validate zoom level (0 to 20)
    if zoom < 0 or zoom > 20:
        return None

    try:
        # Use mercantile library to get tile coordinates
        # Note: mercantile.tile takes (lng, lat, zoom) in that order
        tile = mercantile.tile(lng, lat, zoom)
        return tile.x, tile.y
    except (ValueError, OverflowError, ZeroDivisionError, Exception):
        # Handle any errors that might occur during tile calculation
        return None


# Create UDF for converting lat/lng to slippy tile coordinates
def create_lat_lng_to_tile_udf(zoom_level):
    """Create a UDF for converting lat/lng to slippy tile coordinates with specified zoom level"""
    return udf(
        lambda lat, lng: lat_lng_to_slippy_tile(lat, lng, zoom_level),
        StructType(
            [
                StructField("tile_x", IntegerType(), True),  # Nullable
                StructField("tile_y", IntegerType(), True),  # Nullable
            ]
        ),
    )


# COMMAND ----------


def process_osm_ways_to_tiles(region, version_id, zoom_level):
    """Process OSM full ways table to extract way_id and tile coordinates"""
    print(f"Processing region: {region}, version: {version_id}, zoom: {zoom_level}")

    # Get the full ways table name
    full_ways_table_name = DBX_TABLE.osm_full_ways(version_id, region)

    if not DBX_TABLE.is_table_exists(full_ways_table_name):
        print(f"  ✗ Table {full_ways_table_name} does not exist, skipping")
        return None, 0

    # Read the OSM full ways table
    osm_ways_df = spark.table(full_ways_table_name)

    # Select only way_id and nodes_lat_lng columns
    osm_ways_df = osm_ways_df.select("way_id", "nodes_lat_lng")

    # Count total way IDs before processing
    total_way_ids = osm_ways_df.count()
    print(f"  Total way IDs in source table: {total_way_ids}")

    # Create optimized UDF that replicates intermediate tile generation logic
    def create_optimized_tile_udf(zoom_level, overlap_factor=0.1):
        """
        Optimized approach:
        1. Find all distinct tiles where way nodes fall
        2. For each distinct tile, check center + 8 adjacent tiles with 10% overlap
        3. Return all tiles where the way falls in expanded bounds
        This replicates the intermediate tile generation logic with tight=False
        """

        def calculate_tiles_for_way(nodes_lat_lng):
            if not nodes_lat_lng:
                return []

            # Find all distinct tiles where the way's nodes fall
            way_tiles = set()
            for node in nodes_lat_lng:
                lat = node["latitude"]
                lng = node["longitude"]

                # Validate coordinates before calling mercantile.tile()
                if lat is None or lng is None:
                    continue

                # Handle NaN values
                if math.isnan(lat) or math.isnan(lng):
                    continue

                # Validate latitude range (-90 to 90)
                if lat < -90 or lat > 90:
                    continue

                # Validate longitude range (-180 to 180)
                if lng < -180 or lng > 180:
                    continue

                try:
                    tile = mercantile.tile(lng, lat, zoom_level)
                    way_tiles.add((tile.x, tile.y))
                except (ValueError, OverflowError, ZeroDivisionError, Exception):
                    # Skip invalid coordinates that cause mercantile errors
                    continue

            # Helper function to validate tile coordinates
            def is_valid_tile_coord(x, y, zoom):
                """Check if tile coordinates are valid for the given zoom level"""
                max_index = 2**zoom - 1
                return 0 <= x <= max_index and 0 <= y <= max_index

            # For each distinct tile, check center + 8 adjacent tiles
            all_candidate_tiles = set()
            for main_tile_x, main_tile_y in way_tiles:
                # Get 9 tiles: center + 8 adjacent
                adjacent_tiles = [
                    (main_tile_x - 1, main_tile_y - 1),  # NW
                    (main_tile_x, main_tile_y - 1),  # N
                    (main_tile_x + 1, main_tile_y - 1),  # NE
                    (main_tile_x - 1, main_tile_y),  # W
                    (main_tile_x, main_tile_y),  # Center
                    (main_tile_x + 1, main_tile_y),  # E
                    (main_tile_x - 1, main_tile_y + 1),  # SW
                    (main_tile_x, main_tile_y + 1),  # S
                    (main_tile_x + 1, main_tile_y + 1),  # SE
                ]
                # Only add valid tile coordinates
                for tile_x, tile_y in adjacent_tiles:
                    if is_valid_tile_coord(tile_x, tile_y, zoom_level):
                        all_candidate_tiles.add((tile_x, tile_y))

            # Check which candidate tiles contain the way when expanded by 10%
            result_tiles = []

            for tile_x, tile_y in all_candidate_tiles:
                try:
                    # Get bounds of the tile
                    tile_bounds = mercantile.bounds(tile_x, tile_y, zoom_level)
                except (ValueError, OverflowError, ZeroDivisionError, Exception):
                    # Skip invalid tile coordinates that cause mercantile errors
                    continue

                # Expand by overlap factor (tight=False)
                lat_delta = tile_bounds.north - tile_bounds.south
                lon_delta = tile_bounds.east - tile_bounds.west

                expanded_north = tile_bounds.north + lat_delta * overlap_factor
                expanded_south = tile_bounds.south - lat_delta * overlap_factor
                expanded_east = tile_bounds.east + lon_delta * overlap_factor
                expanded_west = tile_bounds.west - lon_delta * overlap_factor

                # Check if any way node falls in the expanded tile
                way_in_tile = False
                for node in nodes_lat_lng:
                    lat = node["latitude"]
                    lng = node["longitude"]

                    # Validate coordinates before comparison
                    if lat is None or lng is None:
                        continue

                    # Handle NaN values
                    if math.isnan(lat) or math.isnan(lng):
                        continue

                    # Validate latitude range (-90 to 90)
                    if lat < -90 or lat > 90:
                        continue

                    # Validate longitude range (-180 to 180)
                    if lng < -180 or lng > 180:
                        continue

                    # Check latitude containment (no wrapping needed)
                    lat_in_tile = expanded_south <= lat <= expanded_north

                    # Check longitude containment with proper wrapping for International Date Line
                    if expanded_west <= expanded_east:
                        # Normal case: no wrapping across date line
                        lng_in_tile = expanded_west <= lng <= expanded_east
                    else:
                        # Wrapping case: tile crosses International Date Line
                        # Longitude is in tile if it's >= expanded_west OR <= expanded_east
                        lng_in_tile = lng >= expanded_west or lng <= expanded_east

                    if lat_in_tile and lng_in_tile:
                        way_in_tile = True
                        break

                if way_in_tile:
                    result_tiles.append({"tile_x": tile_x, "tile_y": tile_y})

            return result_tiles

        return udf(
            calculate_tiles_for_way,
            ArrayType(
                StructType(
                    [
                        StructField("tile_x", IntegerType(), True),
                        StructField("tile_y", IntegerType(), True),
                    ]
                )
            ),
        )

    # Use the optimized UDF to calculate tiles with 10% overlap
    way_tiles_df = osm_ways_df.withColumn(
        "tile_coordinates", create_optimized_tile_udf(zoom_level)(col("nodes_lat_lng"))
    )

    # Add region and version columns for tracking
    way_tiles_df = way_tiles_df.withColumn("region", lit(region)).withColumn(
        "version_id", lit(version_id)
    )

    processed_way_ids = way_tiles_df.count()
    print(f"  ✓ Processed {processed_way_ids} ways for region {region}")
    return way_tiles_df, total_way_ids


# COMMAND ----------

# Process all regions and combine results
all_way_tiles_dfs = []
total_way_ids_before_processing = 0
total_way_ids_after_processing = 0

for region, version_id in osm_regions_to_version.items():
    way_tiles_df, region_total_way_ids = process_osm_ways_to_tiles(
        region, version_id, zoom_level
    )
    if way_tiles_df is not None:
        all_way_tiles_dfs.append(way_tiles_df)
        total_way_ids_before_processing += region_total_way_ids
        total_way_ids_after_processing += way_tiles_df.count()

# Combine all DataFrames
if all_way_tiles_dfs:
    combined_way_tiles_df = all_way_tiles_dfs[0]
    for df in all_way_tiles_dfs[1:]:
        combined_way_tiles_df = combined_way_tiles_df.union(df)

    print(f"Combined DataFrame has {combined_way_tiles_df.count()} total ways")
else:
    print("No valid DataFrames to combine")
    exit_notebook("No valid OSM full ways tables found")

# COMMAND ----------

# Generate the table name
osm_ways_tiles_table_name = DBX_TABLE.osm_ways_tiles(
    osm_regions_to_version, zoom_level=zoom_level
)
print(f"Writing to table: {osm_ways_tiles_table_name}")

# Select only way_id and tile_coordinates for final table
final_df = combined_way_tiles_df.select("way_id", "tile_coordinates")

# Write to table
final_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(
    osm_ways_tiles_table_name
)

print(f"✓ Successfully wrote {final_df.count()} ways to {osm_ways_tiles_table_name}")

# COMMAND ----------

# Debugging summary
print("=" * 80)
print("DEBUGGING SUMMARY:")
print(f"Total way IDs before computing tiles: {total_way_ids_before_processing}")
print(
    f"Total way IDs for which we have computed tiles: {total_way_ids_after_processing}"
)
if total_way_ids_before_processing > 0:
    success_rate = (
        total_way_ids_after_processing / total_way_ids_before_processing
    ) * 100
    print(f"Success rate: {success_rate:.2f}%")
    if total_way_ids_before_processing > total_way_ids_after_processing:
        lost_ways = total_way_ids_before_processing - total_way_ids_after_processing
        print(f"Lost {lost_ways} ways due to invalid coordinates or processing issues")
print("=" * 80)

# COMMAND ----------

# Display sample of the results
print("Sample of the generated data:")
final_df.show(5, truncate=False)


exit_notebook()
