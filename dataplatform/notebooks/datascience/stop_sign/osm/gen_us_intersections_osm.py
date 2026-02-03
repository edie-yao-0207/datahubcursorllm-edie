# Databricks notebook source
# MAGIC %md
# MAGIC ## Helper Notebook

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find Nodes That Are Shared By Multiple Ways

# COMMAND ----------

osm_us_sdf = spark.table("playground.osm_us_way_nodes")
osm_us_int_sdf = osm_us_sdf.groupBy(
    "node_id", "latitude", "longitude", "node_name", "hex_id", "hex_id_res_13"
).agg(
    collect_set(
        struct(
            col("way_id"),
            col("way_name"),
            col("bearing"),
            col("reverse_bearing"),
            col("highway_tag_value"),
            col("fixed_street"),
        ).alias("way_info")
    ).alias("intersecting_ways"),
    count(col("way_id")).alias("way_count"),
)
osm_us_int_sdf = osm_us_int_sdf.filter(col("way_count") > 1)

# COMMAND ----------


@udf("boolean")
def exclude_same_way(intersecting_ways):
    way_name = intersecting_ways[0].way_name
    way_bearing = intersecting_ways[0].bearing
    for way in intersecting_ways:
        if way.way_name != way_name:
            return False
        if not get_bearing_within_lim(
            way.bearing, way_bearing, 15
        ) and not get_bearing_within_lim(way.reverse_bearing, way_bearing, 15):
            return False
    return True


# COMMAND ----------

osm_us_int_sdf = osm_us_int_sdf.withColumn(
    "exclude_same_way", exclude_same_way(col("intersecting_ways"))
)
osm_us_int_sdf = osm_us_int_sdf.filter(col("exclude_same_way") == False).drop(
    "exclude_same_way"
)

# COMMAND ----------

osm_us_int_sdf.write.mode("overwrite").saveAsTable("stopsigns.osm_us_intersections")
