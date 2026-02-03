# Databricks notebook source
# MAGIC %md
# MAGIC # OSM Parquet to Databricks Converter
# MAGIC This notebook converts a parquetized OSM file to a single databricks table.
# MAGIC
# MAGIC The input should be the s3 prefix containing the parquetized osm.pbf (should have a `way.parquet` and `node.parquet` file), as well as the name of the original `.osm.pbf` file.
# MAGIC
# MAGIC The output columns are as follows:
# MAGIC
# MAGIC | Column        | Description                                                           |
# MAGIC |---------------|-----------------------------------------------------------------------|
# MAGIC | way_id        |                                                                       |
# MAGIC | tags          | key/value pair tags associated with the way_id                        |
# MAGIC | nodes         | array of nodes associated with the way_id                             |
# MAGIC | nodes_lat_lng | array of lat/lng coordinates corresponding to each node in the way_id |
# MAGIC
# MAGIC Note that way_ids containing only one node are considered invalid and are excluded.

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

import json
import os

from pyspark.sql.functions import col, collect_list, explode, size, struct, udf
from pyspark.sql.types import ArrayType, MapType, StringType

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

# COMMAND ----------


def decode_tags(tags):
    """
    decodes utf-8 encoded tags as part of the parquetized osm table.
    """
    decode_tag = lambda tag: {
        "key": tag.key.decode("utf-8"),
        "value": tag.value.decode("utf-8"),
    }
    return [decode_tag(x) for x in tags]


decode_tags_udf = udf(decode_tags, ArrayType(MapType(StringType(), StringType())))


def extract_maxspeed(decoded_tags):
    """
    extracts the maxspeed, returning null if it does not exist
    """
    maxspeed = list(filter(lambda t: t["key"] == "maxspeed", decoded_tags))
    if len(maxspeed) == 0:
        return None
    return maxspeed[0]["value"]


def extract_highway(decoded_tags):
    """
    extracts the highway tag, returning null if it does not exist
    """
    highway = list(filter(lambda t: t["key"] == "highway", decoded_tags))
    if len(highway) == 0:
        return None
    return highway[0]["value"]


extract_highway_udf = udf(extract_highway, StringType())
extract_maxspeed_udf = udf(extract_maxspeed, StringType())

# COMMAND ----------


def osm_parquet_to_databricks_table(version_id, region):
    """
    converts the osm way and node parquet files into a single databricks table for a particular version_id and region.
    """
    print(f"Extracting for {region}, {version_id}")
    volume_prefix = OSM_S3.make_volume_path(version_id, region)
    ways_sdf = (
        sqlContext.read.parquet(volume_prefix + ".way.parquet")
        .withColumnRenamed("id", "way_id")
        .drop("version", "timestamp", "changeset", "uid", "user_sid")
    )
    nodes_sdf = sqlContext.read.parquet(volume_prefix + ".node.parquet").drop(
        "version", "timestamp", "changeset", "uid", "user_sid"
    )

    # Get rid of ways with only one node as anomalies
    ways_sdf = ways_sdf.filter(size(col("nodes")) > 1)

    # Decode binary tags
    ways_tags_sdf = ways_sdf.withColumn("tags", decode_tags_udf(col("tags"))).select(
        "way_id", "nodes", "tags"
    )

    # Generate exploded nodes to simplify mapping nodes to lat/lng
    ways_nodes_exploded_sdf = ways_sdf.withColumn(
        "nodes_exploded", explode(col("nodes"))
    ).select("way_id", "nodes_exploded")
    ways_nodes_exploded_sdf = ways_nodes_exploded_sdf.withColumn(
        "node_id", ways_nodes_exploded_sdf.nodes_exploded.nodeId
    )

    # Join with the nodes table to get lat/lng, then group by way_id to get lat/lng nodes
    ways_with_node = ways_nodes_exploded_sdf.join(
        nodes_sdf, ways_nodes_exploded_sdf.node_id == nodes_sdf.id
    ).drop("id")
    ways_with_node = ways_with_node.groupBy("way_id").agg(
        collect_list(
            struct(
                col("nodes_exploded").index.alias("ind"),
                col("latitude"),
                col("longitude"),
            )
        ).alias("nodes_lat_lng")
    )

    # Finally, join back with ways_tags_sdf to get output with way_ids, tags, and coordinate nodes
    ways_tags_nodes_lat_lng = (
        ways_tags_sdf.alias("a")
        .join(ways_with_node.alias("b"), col("a.way_id") == col("b.way_id"))
        .select(col("a.way_id"), col("a.tags"), col("a.nodes"), col("b.nodes_lat_lng"))
    )

    # Not all node references will match with a row in the nodes table. Exclude ways which have 1 or fewer match.
    # This is important since wkt linestrings do not support a single point, so these entries will cause an error.
    ways_tags_nodes_lat_lng = ways_tags_nodes_lat_lng.filter(
        size(col("nodes_lat_lng")) > 1
    )

    # Extract the maxspeed tag
    ways_tags_nodes_lat_lng = ways_tags_nodes_lat_lng.withColumn(
        "maxspeed", extract_maxspeed_udf(col("tags"))
    )

    # Extract the highway tag
    ways_tags_nodes_lat_lng = ways_tags_nodes_lat_lng.withColumn(
        "highway", extract_highway_udf("tags")
    )

    ways_tags_nodes_lat_lng.write.mode("overwrite").option(
        "overwriteSchema", "True"
    ).saveAsTable(DBX_TABLE.osm_full_ways(version_id, region))


# COMMAND ----------

for region, version_id in osm_regions_to_version.items():
    osm_full_table = DBX_TABLE.osm_full_ways(version_id, region)
    if DBX_TABLE.is_table_exists(osm_full_table):
        print(f"table {osm_full_table} already exists. Skipping {region}, {version_id}")
        continue
    osm_parquet_to_databricks_table(version_id, region)

exit_notebook()

# COMMAND ----------
