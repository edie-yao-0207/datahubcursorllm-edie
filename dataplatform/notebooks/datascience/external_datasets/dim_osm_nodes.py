# Databricks notebook source
import datetime

import h3
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType


@udf("string")
def lat_lng_to_h3_str_udf(lat, lng, res):
    return h3.geo_to_h3(lat, lng, res)


def h3_hex_ring_with_original(hex_id, k=1):
    """
    Returns hex ring as well as the original hex
    Enables us to explode this col and join on it
    """
    o = h3.hex_ring(hex_id, k)
    o.add(hex_id)
    return list(o)


h3_hex_ring_with_original_udf = F.udf(
    h3_hex_ring_with_original, ArrayType(StringType())
)

# COMMAND ----------

nodes_sdf = spark.table("datascience.raw_osm_nodes")
nodes_sdf = nodes_sdf.filter(
    ((nodes_sdf.sam_osm_version == "20200420") & (nodes_sdf.region == "USA"))
)
nodes_sdf = nodes_sdf.withColumn(
    "h3_hex_res_3",
    lat_lng_to_h3_str_udf(nodes_sdf.latitude, nodes_sdf.longitude, F.lit(3)),
)
nodes_sdf = nodes_sdf.withColumn(
    "h3_hex_res_13",
    lat_lng_to_h3_str_udf(nodes_sdf.latitude, nodes_sdf.longitude, F.lit(13)),
)

nodes_sdf = nodes_sdf.withColumn(
    "h3_hex_res_13_ring_and_center",
    h3_hex_ring_with_original_udf(nodes_sdf.h3_hex_res_13),
)

# COMMAND ----------

loc = spark.table("kinesisstats.location")
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
l28 = yesterday - datetime.timedelta(days=28)
yesterday = yesterday.strftime("%Y-%m-%d")
l28 = l28.strftime("%Y-%m-%d")

loc = loc.filter((loc.date <= yesterday) & (loc.date >= l28))
loc = loc.withColumn(
    "hex_id_res_13",
    lat_lng_to_h3_str_udf(loc.value.latitude, loc.value.longitude, F.lit(13)),
)
loc.createOrReplaceTempView("locations")
nodes_sdf.createOrReplaceTempView("nodes")
nodes_with_altitude = spark.sql(
    """
  WITH nodes_exploded AS (
    SELECT
      nodes.*,
      EXPLODE(nodes.h3_hex_res_13_ring_and_center) AS h3_res_13_ring_hex
    FROM nodes
  ),
  altitude AS (
    SELECT
      ne.node_id,
      APPROX_PERCENTILE(value.altitude_meters, 0.5) AS median_altitude_meters,
      AVG(value.altitude_meters) AS mean_altitude_meters,
      MIN(value.altitude_meters) AS min_altitude_meters,
      MAX(value.altitude_meters) AS max_altitude_meters,
      STDDEV(value.altitude_meters) AS stddev_altitude_meters,
      COUNT(1) AS n_obs
    FROM nodes_exploded ne
    JOIN locations l
      ON ne.h3_res_13_ring_hex = l.hex_id_res_13
    GROUP BY ne.node_id
  )

  SELECT
    n.*,
    a.median_altitude_meters,
    a.mean_altitude_meters,
    a.min_altitude_meters,
    a.max_altitude_meters,
    a.stddev_altitude_meters,
    a.n_obs
  FROM nodes n
  LEFT JOIN altitude a
    ON a.node_id = n.node_id
"""
)

nodes_with_altitude.head()


# COMMAND ----------

db_name = "datascience"
dim_table_name = "dim_osm_nodes"

if spark._jsparkSession.catalog().tableExists(db_name, dim_table_name):
    nodes_with_altitude.createOrReplaceTempView("to_merge")
    spark.sql(
        f"""
    MERGE INTO {db_name}.{dim_table_name} c
    USING to_merge tm
    ON
      tm.node_id = c.node_id
      AND tm.sam_osm_version = c.sam_osm_version
      AND tm.region = c.region
    WHEN MATCHED
      THEN UPDATE SET *
    WHEN NOT MATCHED
      THEN INSERT *
  """
    )
else:
    nodes_with_altitude.write.format("delta").partitionBy(
        ["sam_osm_version", "region"]
    ).saveAsTable(f"{db_name}.{dim_table_name}")
