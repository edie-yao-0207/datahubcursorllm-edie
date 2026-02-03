# Databricks notebook source
@udf("boolean")
def get_same_dir(b1, b2):
    r = min((b2 - b1) % 360.0, (b1 - b2) % 360)
    return r <= 90


@udf("long")
def get_node_to_remove(n1, n2, same_dir, bearing_dir):
    if (same_dir and bearing_dir == 1) or (not same_dir and bearing_dir == -1):
        return n2
    else:
        return n1


# COMMAND ----------

# MAGIC %run /backend/datascience/stop_sign/utils

# COMMAND ----------

from pyspark.sql import functions as F


class CloseStopSignIdentifier:
    def __init__(self, region, stop_min_conf_thresh):
        self.region = region
        self.stop_min_conf_thresh = stop_min_conf_thresh

    def get_close_stops_to_remove(self):
        cs_df = self.get_close_stop_signs()
        to_remove = self.get_one_is_service(cs_df)
        to_remove = to_remove.unionAll(self.get_same_name_two_crossings(cs_df))
        to_remove_final = to_remove.groupBy(
            [
                "node_id",
                "way_id",
                "bearing",
                "bearing_dir",
            ]
        ).agg(F.collect_list("removal_reason").alias("removal_reasons"))

        return to_remove_final

    def get_close_stop_signs(self):
        q = f"""
        WITH stops AS (
          SELECT
            DISTINCT
              node_id,
              way_id,
              bearing_dir,
              bearing,
              latitude,
              longitude,
              combined_prediction
          FROM
            stopsigns.combined_preds_{self.region}_all
          WHERE combined_prediction >= {self.stop_min_conf_thresh}
        ),
        close_stops AS (
          SELECT
            DISTINCT
            isec1.node_id as node_id_1,
            isec1.latitude as lat_1,
            isec1.longitude as lon_1,
            isec2.node_id as node_id_2,
            isec2.latitude as lat_2,
            isec2.longitude as lon_2,
            isec1.intersecting_ways AS iways_1,
            isec2.intersecting_ways AS iways_2,
            s1.way_id,
            s1.bearing_dir,
            s1.bearing
          FROM stopsigns.{self.region}_intersections_exploded_h3 as isec1
          JOIN stopsigns.{self.region}_intersections_exploded_h3 as isec2
            ON isec1.node_id > isec2.node_id
            AND isec1.bounding_box_exploded_h3 = isec2.bounding_box_exploded_h3
          JOIN stops AS s1
            ON s1.node_id = isec1.node_id
          JOIN stops AS s2
            ON s2.node_id = isec2.node_id
              AND s1.way_id = s2.way_id
              AND s1.bearing_dir = s2.bearing_dir
        )
        SELECT
          *
        FROM close_stops
        """
        return spark.sql(q)

    def get_one_is_service(self, close_stop_df):
        # We remove all close stops where one of the roads
        # is a service road and the other road is not
        close_stop_df.createOrReplaceTempView("cs_df")
        q = """
      WITH cs AS (
        SELECT
          node_id_1,
          node_id_2,
          way_id,
          bearing,
          bearing_dir,
          array_contains(TRANSFORM(iways_1, x -> x.highway_tag_value), 'service') AS ways_1_service,
          array_contains(TRANSFORM(iways_2, x -> x.highway_tag_value), 'service') AS ways_2_service
        FROM
          cs_df
      ),
      final AS (
        SELECT
          node_id_1 as node_id,
          way_id,
          bearing,
          bearing_dir
        FROM cs
        WHERE
          ways_1_service
          AND NOT ways_2_service
        UNION
        SELECT
          node_id_2 AS node_id,
          way_id,
          bearing,
          bearing_dir
        FROM cs
        WHERE
          ways_2_service
          AND NOT ways_1_service
       )
       SELECT
         node_id,
         way_id,
         bearing,
         bearing_dir,
         "one_is_service" AS removal_reason
       FROM
         final
    """
        return spark.sql(q)

    def get_same_name_two_crossings(self, close_stop_df):
        close_stop_df.createOrReplaceTempView("cs_df")
        q = """
      WITH cs AS (
        SELECT
          node_id_1,
          node_id_2,
          lat_1,
          lon_1,
          lat_2,
          lon_2,
          way_id,
          FILTER(iways_1, x -> x.way_id == way_id)[0].way_name AS way_name,
          bearing,
          bearing_dir,
          array_contains(TRANSFORM(iways_1, x -> x.highway_tag_value), 'service') AS ways_1_service,
          array_contains(TRANSFORM(iways_2, x -> x.highway_tag_value), 'service') AS ways_2_service,
          TRANSFORM(iways_1, x -> x.way_name) AS way_names_1,
          TRANSFORM(iways_2, x -> x.way_name) AS way_names_2
        FROM
          cs_df
      ),
      cs2 AS (
        SELECT
          node_id_1,
          node_id_2,
          lat_1,
          lon_1,
          lat_2,
          lon_2,
          way_id,
          bearing,
          bearing_dir,
          CARDINALITY(
            ARRAY_INTERSECT(
              FILTER(way_names_1, x -> x != way_name AND x != ""),
              FILTER(way_names_2, x -> x != way_name AND x != "")
            )
          ) > 0 AS same_name
        FROM cs
        WHERE
          NOT ways_1_service
          AND NOT ways_2_service
      )
      SELECT
        node_id_1,
        node_id_2,
        lat_1,
        lon_1,
        lat_2,
        lon_2,
        way_id,
        bearing,
        bearing_dir
      FROM cs2
      WHERE same_name
    """
        sdf_raw = spark.sql(q)
        sdf_w_bearing = sdf_raw.withColumn(
            "one_to_two_bearing",
            calculate_bearing(
                F.col("lon_1"), F.col("lat_1"), F.col("lon_2"), F.col("lat_2")
            ),
        )
        sdf_w_samedir = sdf_w_bearing.withColumn(
            "same_dir", get_same_dir(F.col("one_to_two_bearing"), F.col("bearing"))
        )
        sdf_with_nodes_to_remove = sdf_w_samedir.withColumn(
            "node_id",
            get_node_to_remove(
                F.col("node_id_1"),
                F.col("node_id_2"),
                F.col("same_dir"),
                F.col("bearing_dir"),
            ),
        )
        sdf_final = (
            sdf_with_nodes_to_remove.withColumn(
                "removal_reason", F.lit("same_name_two_crossings")
            )
            .select(
                [
                    "node_id",
                    "way_id",
                    "bearing",
                    "bearing_dir",
                    "removal_reason",
                ]
            )
            .distinct()
        )
        return sdf_final
