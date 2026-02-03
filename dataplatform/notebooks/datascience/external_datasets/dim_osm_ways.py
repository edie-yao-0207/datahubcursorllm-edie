# Databricks notebook source
# Update data sets
latest_speed_limit_table = "safety_map_data.osm_can_20200420_eur_20201102_mex_20200420_usa_20201110__tomtom_202103000__resolved_speed_limits"

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, LongType, StringType


def extract_tag(tags, key):
    """
    Extracts highway tag
    """
    for i in tags:
        if i["key"] == key:
            return i["value"]
    return None


extract_tag_udf = F.udf(extract_tag, StringType())


def add_tag_column(sdf, tag_key):
    return sdf.withColumn(
        f"{tag_key.replace(':', '_')}_tag", extract_tag_udf(sdf.tags, F.lit(tag_key))
    )


def get_sorted_nodes(nodes):
    """
    Extract raw nodes data and ensure that it's sorted
    """
    nodes = [(n["index"], n["nodeId"]) for n in nodes]
    nodes.sort()
    return [n[1] for n in nodes]


get_sorted_nodes_udf = F.udf(get_sorted_nodes, ArrayType(LongType()))

# COMMAND ----------

ways_sdf = spark.table("datascience.raw_osm_ways")
ways_sdf = ways_sdf.filter(
    ((ways_sdf.sam_osm_version == "20200420") & (ways_sdf.region == "USA"))
)

# Extract Tags
ways_sdf = add_tag_column(ways_sdf, "highway")
ways_sdf = add_tag_column(ways_sdf, "private")
ways_sdf = add_tag_column(ways_sdf, "surface")
ways_sdf = add_tag_column(ways_sdf, "lanes")
ways_sdf = add_tag_column(ways_sdf, "lanes:forward")
ways_sdf = add_tag_column(ways_sdf, "lanes:backward")
ways_sdf = add_tag_column(ways_sdf, "name")
ways_sdf = ways_sdf.drop("tags")

# Handle Nodes
ways_sdf = ways_sdf.withColumn("nodes", get_sorted_nodes_udf(ways_sdf.nodes))

ways_sdf = ways_sdf.withColumn("num_nodes", F.size(ways_sdf.nodes))

# Join Speed Limit Data
ways_sdf.createOrReplaceTempView("ways")
ways_sdf = spark.sql(
    f"""
    WITH speed_limits_raw AS (
        SELECT
            osm_way_id,
            tomtom_country_code AS country_code,
            CASE
            WHEN LOWER(SUBSTR(osm_tomtom_updated_passenger_limit, -3)) = 'kph'
            AND NOT osm_tomtom_updated_passenger_limit RLIKE ';' THEN CAST(
                SUBSTR(
                osm_tomtom_updated_passenger_limit,
                0,
                LENGTH(osm_tomtom_updated_passenger_limit) - 4
                ) AS BIGINT
            ) * 0.277778
            WHEN LOWER(SUBSTR(osm_tomtom_updated_passenger_limit, -3)) = 'mph'
            AND NOT osm_tomtom_updated_passenger_limit RLIKE ';' THEN CAST(
                SUBSTR(
                osm_tomtom_updated_passenger_limit,
                0,
                LENGTH(osm_tomtom_updated_passenger_limit) - 4
                ) AS BIGINT
            ) * 0.44704
            ELSE IF(
                tomtom_maxspeed_unit = 'kph',
                tomtom_maxspeed * 0.277778,
                tomtom_maxspeed * 0.44704
            )
            END AS speed_limit_mps,
            TRANSFORM_VALUES(
            tomtom_commercial_vehicle_speed_map,
            (k, v) -> IF(
                SUBSTR(v, -3) = 'kph',
                CAST(SUBSTR(v, 0, LENGTH(v) - 4) AS BIGINT) * 0.277778,
                CAST(SUBSTR(v, 0, LENGTH(v) - 4) AS BIGINT) * 0.44704
            )
            ) AS commercial_vehicle_speed_limit_mps_map,
            ROW_NUMBER() OVER(
            PARTITION BY osm_way_id,
            tomtom_country_code
            ORDER BY
                osm_way_id
            ) AS rn
        FROM
            {latest_speed_limit_table}
        WHERE
            tomtom_country_code IS NOT NULL
        ),
        speed_limits AS (
            SELECT
                osm_way_id,
                MAP_FROM_ENTRIES(
                COLLECT_LIST(
                    STRUCT(
                    country_code,
                    speed_limit_mps
                    )
                )
                ) AS speed_limit_mps_by_country,
                MAP_FROM_ENTRIES(
                COLLECT_LIST(
                    STRUCT(
                    country_code,
                    commercial_vehicle_speed_limit_mps_map
                    )
                )
                ) AS commercial_vehicle_speed_limit_mps_map_by_country
            FROM
                speed_limits_raw
            WHERE
                rn = 1
            GROUP BY
                osm_way_id
            )
    SELECT
        w.*,
        sl.speed_limit_mps_by_country,
        sl.commercial_vehicle_speed_limit_mps_map_by_country
    FROM ways w
    LEFT JOIN speed_limits sl
        ON sl.osm_way_id = w.way_id
    WHERE w.num_nodes > 1
    AND (w.highway_tag IS NOT NULL OR w.private_tag IS NOT NULL)
"""
)

ways_sdf = ways_sdf.withColumn(
    "speed_limit_datasource", F.lit(latest_speed_limit_table)
)

# COMMAND ----------

db_name = "datascience"
dim_table_name = "dim_osm_ways"

if spark._jsparkSession.catalog().tableExists(db_name, dim_table_name):
    ways_sdf.createOrReplaceTempView("to_merge")
    spark.sql(
        f"""
    MERGE INTO {db_name}.{dim_table_name} c
    USING to_merge tm
    ON
      tm.way_id = c.way_id
      AND tm.sam_osm_version = c.sam_osm_version
      AND tm.region = c.region
    WHEN MATCHED
      THEN UPDATE SET *
    WHEN NOT MATCHED
      THEN INSERT *
  """
    )
else:
    ways_sdf.write.format("delta").partitionBy(
        ["sam_osm_version", "region"]
    ).saveAsTable(f"{db_name}.{dim_table_name}")
