# Databricks notebook source
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_dataset_write

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/map_match_utils

# COMMAND ----------
"""
Purpose of this notebook:

This notebook generates the datasets necessary to build debug editor tiles
These Debug Editor tiles give us a visual of how our map-matching algorithm
is working by showing us for a given way_id which tomtom way was selected
and what were the potential options that it had to evaluate against.
"""

"""
This notebook takes in the following args:
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
* ARG_TOMTOM_VERSION:
Must be specified in YYMM000 format
"""
dbutils.widgets.text(ARG_TOMTOM_VERSION, "")
tomtom_version = dbutils.widgets.get(ARG_TOMTOM_VERSION)
print(f"{ARG_TOMTOM_VERSION}: {tomtom_version}")


"""
* ARG_TILE_VERSION:
Integer tile version
"""
dbutils.widgets.text(ARG_TILE_VERSION, "")
tile_version = dbutils.widgets.get(ARG_TILE_VERSION)
print(f"{ARG_TILE_VERSION}: {tile_version}")


"""
* ARG_IS_TOMTOM_DECOUPLED:
Boolean flag to indicate if the tomtom data is decoupled
"""
dbutils.widgets.text(ARG_IS_TOMTOM_DECOUPLED, "")
is_tomtom_decoupled = dbutils.widgets.get(ARG_IS_TOMTOM_DECOUPLED).lower() == "true"
print(f"{ARG_IS_TOMTOM_DECOUPLED}: {is_tomtom_decoupled}")


"""
* ARG_SPEED_LIMIT_DATASET_VERSION:
Integer dataset version
"""
dbutils.widgets.text(ARG_SPEED_LIMIT_DATASET_VERSION, "")
tomtom_dataset_version = dbutils.widgets.get(ARG_SPEED_LIMIT_DATASET_VERSION)
print(f"{ARG_SPEED_LIMIT_DATASET_VERSION}: {tomtom_dataset_version}")

# COMMAND ----------

# Utils functions
from functools import reduce
import math

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from shapely import wkt


def convert_to_kmph(limit, unit):
    # concat and convert to kmph
    if limit is None:
        return None
    if unit == "kmph" or unit == "kph":
        return f"{limit} kmph"
    limit *= 1.60934
    return f"{limit} kmph"


convert_to_kmph_udf = udf(convert_to_kmph, StringType())


def deg2num(lat_deg, lon_deg):
    zoom = 15
    lat_rad = math.radians(lat_deg)
    n = 2.0**zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return (xtile, ytile)


def make_slippy_coordinates(line_string):
    g = wkt.loads(line_string)

    first_point = g.coords[0]
    return deg2num(first_point[1], first_point[0])


schema = StructType(
    [StructField("x", IntegerType(), False), StructField("y", IntegerType(), False)]
)

make_slippy_coordinates_udf = udf(make_slippy_coordinates, schema)

# COMMAND ----------

# processing DFs
# Get all the tomtom ways and add slippy x, y coordinates for each way
def tomtom_ways_df(table):
    if not DBX_TABLE.is_table_exists(table):
        exit_notebook(f"Missing TomTom Full Ways table: {table}")

    tomtom_df = spark.table(table)
    tomtom_df = tomtom_df.filter(col("speed_restrictions").isNotNull()).withColumn(
        "maxspeed", max_passenger_speed_udf(col("speed_restrictions"))
    )
    tomtom_df = tomtom_df.withColumn(
        "speed_limit", convert_to_kmph_udf(col("maxspeed"), col("maxspeed_unit"))
    )
    tomtom_df = tomtom_df.withColumn("slippy", make_slippy_coordinates_udf(col("wkt")))
    tomtom_df = tomtom_df.withColumn("slippy_x", col("slippy.x"))
    tomtom_df = tomtom_df.withColumn("slippy_y", col("slippy.y"))
    tomtom_df.drop("slippy")

    return tomtom_df.select(
        col("id").alias("tomtom_way_id"), "wkt", "speed_limit", "slippy_x", "slippy_y"
    )


# create a df of all of the potential osm -> tomtom matches
def get_potential_tomtom_matches(table_name):
    if not DBX_TABLE.is_table_exists(table_name):
        exit_notebook(f"Missing OSM to TomTom Matched ways table: {table_name}")

    match_df = spark.table(table_name)
    return match_df.select("osm_way_id", "tomtom_way_id").where(
        col("osm_way_id").isNotNull()
    )


# grab the final osm -> tomtom ways map match
def get_final_osm_to_tomtom_map_match(table_name):
    if not DBX_TABLE.is_table_exists(table_name):
        exit_notebook(f"Missing TomTom Full Ways table: {table_name}")

    match_df = spark.table(table_name)
    return match_df.select("osm_way_id", "tomtom_way_id").where(
        col("osm_way_id").isNotNull()
    )


# Join the potential osm to tomtom matches df with the
# final osm to tomtom matches df
def join_map_match_dfs(potential_match_df, final_match_df):
    final_match_df = final_match_df.withColumnRenamed(
        "osm_way_id", "osm_way_id_2"
    ).withColumnRenamed("tomtom_way_id", "tomtom_way_id_2")
    df = potential_match_df.join(
        final_match_df,
        (potential_match_df.osm_way_id == final_match_df.osm_way_id_2)
        & (potential_match_df.tomtom_way_id == final_match_df.tomtom_way_id_2),
        "left",
    )
    df = df.withColumn(
        "is_selected",
        (col("osm_way_id_2").isNotNull() & col("tomtom_way_id_2").isNotNull()).cast(
            "integer"
        ),
    )
    df = df.drop("osm_way_id_2", "tomtom_way_id_2")
    return df.where(col("tomtom_way_id").isNotNull())


# COMMAND ----------

# Create a DF of all of the TomTom Ways w/slippy_x_y coordinates
regions = osm_regions_to_version.keys()
regional_dfs = []
for region in regions:
    tomtom_full_ways_table = DBX_TABLE.tomtom_full_ways(tomtom_version, region)
    if is_tomtom_decoupled:
        tomtom_full_ways_table = DBX_TABLE.tomtom_full_ways_decoupled(
            tomtom_version, region
        )
    print(tomtom_full_ways_table)
    regional_df = tomtom_ways_df(tomtom_full_ways_table)
    regional_dfs.append(regional_df)
all_tomtom_ways_df = reduce(DataFrame.unionAll, regional_dfs)

# Persist all the tomtom data to S3
df = all_tomtom_ways_df.drop_duplicates(["tomtom_way_id"])
persist_tomtom_to_sqlite(df, tile_version, is_tomtom_decoupled, tomtom_dataset_version)


# Create a DF of all of the Potential OSM to TomTom Matches
regional_dfs = []
for region, osm_version_id in osm_regions_to_version.items():
    match_table_name = DBX_TABLE.osm_tomtom_matched_ways(
        osm_version_id, region, tomtom_version
    )
    if is_tomtom_decoupled:
        match_table_name += "_decoupled"
    print(match_table_name)
    regional_df = get_potential_tomtom_matches(match_table_name)
    regional_dfs.append(regional_df)
all_match_dfs = reduce(DataFrame.unionAll, regional_dfs)


# Create a DF of all of the final osm to tomtom matches
regional_dfs = []
for region, osm_version_id in osm_regions_to_version.items():
    match_table_name = DBX_TABLE.osm_tomtom_map_match(
        osm_version_id, region, tomtom_version
    )
    if is_tomtom_decoupled:
        match_table_name += "_decoupled"
    print(match_table_name)
    regional_df = get_final_osm_to_tomtom_map_match(match_table_name)
    regional_dfs.append(regional_df)
all_final_match_dfs = reduce(DataFrame.unionAll, regional_dfs)

# Join the potential matches with the final matches
osm_to_tomtom_matches_df = join_map_match_dfs(all_match_dfs, all_final_match_dfs)

# Write out matches to S3
persist_osm_to_tomtom_matches(
    osm_to_tomtom_matches_df, tile_version, is_tomtom_decoupled, tomtom_dataset_version
)

# COMMAND ----------

exit_notebook()


# COMMAND ----------
