# Databricks notebook source
# MAGIC %md
# MAGIC ## Run helper notebook

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load OSM Ways Data For The US

# COMMAND ----------

f = "/mnt/samsara-databricks-playground/stop_sign_detection/osm/us-latest-20201110.osm.pbf.way.parquet"
ways_sdf = sqlContext.read.parquet(f).drop(
    "version", "timestamp", "changeset", "uid", "user_sid"
)
ways_sdf = ways_sdf.withColumnRenamed("id", "way_id")

# COMMAND ----------

# Get rid of ways with only one node as anomalies
ways_sdf = ways_sdf.select("way_id", "nodes", "tags")
ways_sdf = ways_sdf.filter(size(col("nodes")) > 1)

# COMMAND ----------

# Decode the tags from base64
way_with_tags_sdf = ways_sdf.withColumn("tags_exploded", explode(col("tags")))
way_with_tags_sdf = way_with_tags_sdf.withColumn(
    "key_decoded", unbase64(base64(way_with_tags_sdf.tags_exploded.key)).cast("string")
)
way_with_tags_sdf = way_with_tags_sdf.withColumn(
    "value_decoded",
    unbase64(base64(way_with_tags_sdf.tags_exploded.value)).cast("string"),
)
way_with_tags_sdf = way_with_tags_sdf.groupBy("way_id").agg(
    collect_list(struct(col("key_decoded"), col("value_decoded"))).alias("tags_decoded")
)

# COMMAND ----------


@udf
def extract_name(tags):
    for tag in tags:
        if tag.key_decoded == "name" or tag.key_decoded == "addr:street":
            return tag.value_decoded
    return ""


@udf
def highway(tags):
    for tag in tags:
        if tag.key_decoded == "highway":
            return True
    return False


@udf("boolean")
def exclude_service(tags):
    exclude_vals = {"driveway", "parking_aisle"}
    for tag in tags:
        if tag.key_decoded == "service" and tag.value_decoded in exclude_vals:
            return True
    return False


@udf("boolean")
def exclude_access(tags):
    exclude_vals = {"private"}
    for tag in tags:
        if tag.key_decoded == "access" and tag.value_decoded in exclude_vals:
            return True
    return False


@udf
def highway_tag_val(tags):
    for tag in tags:
        if tag.key_decoded == "highway":
            return tag.value_decoded
    return ""


@udf("boolean")
def exlcude_way(highway_val):
    exclude_vals = {
        "footway",
        "living_street",
        "pedestrian",
        "track",
        "bridleway",
        "steps",
        "corridor",
        "path",
        "sidewalk",
        "crossing",
        "cycleway",
        "elevator",
        "emergency_bay",
        "emergency_access_point",
        "trailhead",
    }
    return highway_val in exclude_vals


@udf
def first_last_node_diff(nodes):
    return nodes[0].nodeId != nodes[-1].nodeId


# COMMAND ----------

# Extract the name
way_with_tags_sdf = way_with_tags_sdf.withColumn(
    "way_name", extract_name(col("tags_decoded"))
)
way_with_tags_sdf = way_with_tags_sdf.withColumn(
    "highway", highway(col("tags_decoded"))
)
way_with_tags_sdf = way_with_tags_sdf.filter(col("highway") == True)
way_with_tags_sdf = way_with_tags_sdf.withColumn(
    "highway_tag_value", highway_tag_val(col("tags_decoded"))
)
way_with_tags_sdf = (
    way_with_tags_sdf.withColumn("exclude_way", exlcude_way(col("highway_tag_value")))
    .filter(col("exclude_way") == False)
    .drop("exclude_way")
)

way_with_tags_sdf = (
    way_with_tags_sdf.withColumn(
        "exclude_service", exclude_service(col("tags_decoded"))
    )
    .filter(col("exclude_service") == False)
    .drop("exclude_service")
)

# COMMAND ----------

ways_sdf = (
    ways_sdf.join(way_with_tags_sdf, "way_id")
    .drop("tags")
    .withColumnRenamed("tags_decoded", "tags")
)

# COMMAND ----------

# Filter out ways that are circular as they will have incorrect bearings
ways_sdf = (
    ways_sdf.withColumn("first_last_node_diff", first_last_node_diff(col("nodes")))
    .filter(col("first_last_node_diff") == True)
    .drop("first_last_node_diff")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load OSM Node Data For The US

# COMMAND ----------
# using OSM version 11/20/2020
f = "/mnt/samsara-databricks-playground/stop_sign_detection/osm/us-latest-20201110.osm.pbf.node.parquet"
nodeDF = sqlContext.read.parquet(f).drop(
    "version", "timestamp", "changeset", "uid", "user_sid"
)

# COMMAND ----------

# Decode tags from base 64
node_with_tags_sdf = nodeDF.withColumn("tags_exploded", explode(col("tags")))
node_with_tags_sdf = node_with_tags_sdf.withColumn(
    "key_decoded", unbase64(base64(node_with_tags_sdf.tags_exploded.key)).cast("string")
)
node_with_tags_sdf = node_with_tags_sdf.withColumn(
    "value_decoded",
    unbase64(base64(node_with_tags_sdf.tags_exploded.value)).cast("string"),
)
node_with_tags_sdf = node_with_tags_sdf.groupBy("id").agg(
    collect_list(struct(col("key_decoded"), col("value_decoded"))).alias("tags_decoded")
)

# COMMAND ----------

node_with_tags_sdf = node_with_tags_sdf.withColumn(
    "node_name", extract_name(col("tags_decoded"))
)

# COMMAND ----------

node_sdf = (
    nodeDF.join(node_with_tags_sdf, "id", how="left")
    .drop("tags")
    .withColumnRenamed("tags_decoded", "node_tags")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Ways Data With Nodes

# COMMAND ----------

ways_sdf = ways_sdf.withColumn("nodes_exploded", explode(col("nodes")))
ways_sdf = ways_sdf.withColumn("node_id", ways_sdf.nodes_exploded.nodeId)

# COMMAND ----------

ways_with_node = ways_sdf.join(node_sdf, ways_sdf.node_id == node_sdf.id).drop("id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Bearing For Each Way

# COMMAND ----------

geod = pyproj.Geod(ellps="WGS84")

# We calculate the bearing of the way from the first & last node
# This is how it is done in our tile generation code in the backend
# We also calculate a reverse bearing for each node by rotating
# the bearing by 180 degrees. This let's us capture the bearing
# for two way streets.
@udf("double")
def calculate_bearing(node_list):
    first_node = node_list[0]
    last_node = node_list[-1]
    for i in range(len(node_list)):
        if node_list[i].nodes_exploded.index == 0:
            first_node = node_list[i]
        if node_list[i].nodes_exploded.index == (len(node_list) - 1):
            last_node = node_list[i]
    # World Geotic System revision 84 (used by GPS).
    bearing, _, _ = geod.inv(
        first_node.longitude,
        first_node.latitude,
        last_node.longitude,
        last_node.latitude,
    )
    return bearing


# COMMAND ----------

# Bearing
ways_with_bearing = ways_with_node.groupBy("way_id").agg(
    collect_list(
        struct(col("nodes_exploded"), col("latitude"), col("longitude"))
    ).alias("node_list")
)
ways_with_bearing = ways_with_bearing.withColumn(
    "bearing", calculate_bearing(col("node_list"))
)
ways_with_bearing = ways_with_bearing.withColumn(
    "reverse_bearing", calculate_reverse_bearing(col("bearing"))
)

# COMMAND ----------

ways_with_node = ways_with_node.join(ways_with_bearing, "way_id").drop("node_list")
ways_with_node = ways_with_node.withColumn("fixed_street", fix_street(col("way_name")))

# COMMAND ----------

# Determined resolution 13 through trial and error
ways_with_h3 = ways_with_h3.withColumn(
    "hex_id_res_13", lat_lng_to_h3_str_udf(col("latitude"), col("longitude"), lit(13))
)

# COMMAND ----------

ways_with_h3.write.mode("overwrite").saveAsTable("stopsigns.osm_us_way_nodes")
