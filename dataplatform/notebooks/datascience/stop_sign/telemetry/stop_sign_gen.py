# Databricks notebook source
# MAGIC %run "/backend/datascience/stop_sign/utils"

# COMMAND ----------


@udf("integer")
def get_closer_bearing(stop_bearing, bearing, reverse_bearing):
    lim = 60
    in_bearing_range = get_bearing_within_lim(bearing, stop_bearing, lim)
    if in_bearing_range:
        return 1
    in_reverse_bearing_range = get_bearing_within_lim(
        reverse_bearing, stop_bearing, lim
    )
    if in_reverse_bearing_range:
        return -1
    while not in_bearing_range and not in_reverse_bearing_range:
        lim = lim + 10
        in_bearing_range = get_bearing_within_lim(bearing, stop_bearing, lim)
        if in_bearing_range:
            return 1
        in_reverse_bearing_range = get_bearing_within_lim(
            reverse_bearing, stop_bearing, lim
        )
        if in_reverse_bearing_range:
            return -1


@udf("boolean")
def osm_align_stop(intsec_bearing, osm_bearing, intsec_bearing_max, intsec_bearing_min):
    if intsec_bearing < -150 or intsec_bearing > 150:
        return osm_bearing <= intsec_bearing_max or osm_bearing >= intsec_bearing_min
    return osm_bearing >= intsec_bearing_min and osm_bearing <= intsec_bearing_max


@udf("boolean")
def equal_names_or_empty(stop_fixed_street, fixed_street):
    if not stop_fixed_street:
        return True
    if not fixed_street:
        return True
    return stop_fixed_street == fixed_street


# COMMAND ----------

from datetime import date
import time

import pyspark.sql.functions as F


class CityStopSignGenerator:
    def __init__(self, city_name, stop_signs_sdf, int_filter_fn):
        self.city_name = city_name
        self.stop_signs_sdf = stop_signs_sdf
        self.int_filter_fn = int_filter_fn

    def gen_bbox_sdf(self):
        stop_signs_h3 = self.stop_signs_sdf.withColumn(
            "hex_id_res_13",
            lat_lng_to_h3_str_udf(col("latitude"), col("longitude"), lit(13)),
        )
        bbox_sdf = gen_bounding_box_and_bearings(stop_signs_h3, include_reverse=False)
        write_table_no_partition(bbox_sdf, f"stopsigns.{self.city_name}_stop_signs")

    def gen_exploded_sdf(self):
        bbox_sdf = spark.table(f"stopsigns.{self.city_name}_stop_signs")
        exploded_h3_sdf = bbox_sdf.withColumn(
            "bounding_box_exploded_h3", explode("bounding_box_h3")
        )
        write_table_no_partition(
            exploded_h3_sdf, f"stopsigns.{self.city_name}_stop_signs_exploded_h3"
        )

    def gen_intersection_stop_boundaries(self):
        int_sdf = spark.table(f"stopsigns.{self.city_name}_intersections")
        filtered_int_sdf = self.int_filter_fn(int_sdf)
        write_table_no_partition(
            filtered_int_sdf,
            f"stopsigns.{self.city_name}_intersections_stop_boundaries",
        )

    def gen_stop_sign_osm(self):
        # rename stop sign cols to avoid confusion w osm cols

        stop_signs_sdf = spark.table(
            f"stopsigns.{self.city_name}_stop_signs_exploded_h3"
        )
        stop_signs_sdf = stop_signs_sdf.withColumnRenamed("latitude", "stop_lat")
        stop_signs_sdf = stop_signs_sdf.withColumnRenamed("longitude", "stop_lon")
        stop_signs_sdf = stop_signs_sdf.withColumnRenamed("bearing", "stop_bearing")
        stop_signs_sdf = stop_signs_sdf.withColumnRenamed(
            "bearing_max", "stop_bearing_max"
        )
        stop_signs_sdf = stop_signs_sdf.withColumnRenamed(
            "bearing_min", "stop_bearing_min"
        )
        stop_signs_sdf = stop_signs_sdf.withColumnRenamed(
            "fixed_street", "stop_fixed_street"
        )

        osm_sdf = spark.table(
            f"stopsigns.{self.city_name}_intersections_stop_boundaries"
        )

        stop_cols = [
            "stop_lat",
            "stop_lon",
            "stop_bearing",
            "stop_bearing_max",
            "stop_bearing_min",
        ]
        osm_cols = [
            osm_sdf.latitude,
            osm_sdf.longitude,
            osm_sdf.way_id,
            osm_sdf.node_id,
            osm_sdf.bearing,
            osm_sdf.reverse_bearing,
            "stop_fixed_street",
        ]
        select_cols = stop_cols + osm_cols

        # join bearing dir 1
        osm_stop_signs_forward = stop_signs_sdf.join(
            osm_sdf, stop_signs_sdf.bounding_box_exploded_h3 == osm_sdf.hex_id_res_13
        ).select(select_cols)
        osm_stop_signs_forward = osm_stop_signs_forward.withColumn(
            "osm_aligned",
            osm_align_stop(
                col("stop_bearing"),
                col("bearing"),
                col("stop_bearing_max"),
                col("stop_bearing_min"),
            ),
        )
        osm_stop_signs_forward = osm_stop_signs_forward.filter(col("osm_aligned")).drop(
            "osm_aligned"
        )
        osm_stop_signs_forward = osm_stop_signs_forward.withColumn(
            "bearing_dir", lit(1)
        )

        # join bearing dir -1
        osm_stop_signs_reverse = stop_signs_sdf.join(
            osm_sdf, stop_signs_sdf.bounding_box_exploded_h3 == osm_sdf.hex_id_res_13
        ).select(select_cols)
        osm_stop_signs_reverse = osm_stop_signs_reverse.withColumn(
            "osm_aligned",
            osm_align_stop(
                col("stop_bearing"),
                col("reverse_bearing"),
                col("stop_bearing_max"),
                col("stop_bearing_min"),
            ),
        )
        osm_stop_signs_reverse = osm_stop_signs_reverse.filter(col("osm_aligned")).drop(
            "osm_aligned"
        )
        osm_stop_signs_reverse = osm_stop_signs_reverse.withColumn(
            "bearing_dir", lit(-1)
        )

        osm_stop_signs = osm_stop_signs_forward.unionByName(
            osm_stop_signs_reverse
        ).withColumn("use_bearing", lit(True))
        osm_stop_signs.createOrReplaceTempView("osm_stop_signs")
        stop_signs_sdf.createOrReplaceTempView("stop_signs")

        unassigned_osm = spark.sql(
            f"""
      select * from stopsigns.{self.city_name}_intersections_stop_boundaries where struct(way_id, node_id) not in (select way_id, node_id from osm_stop_signs)"""
        )

        osm_stop_by_name = (
            stop_signs_sdf.join(
                unassigned_osm,
                (
                    stop_signs_sdf.bounding_box_exploded_h3
                    == unassigned_osm.hex_id_res_13
                )
                & (stop_signs_sdf.stop_fixed_street == unassigned_osm.fixed_street),
            )
            .select(
                [
                    unassigned_osm.latitude,
                    unassigned_osm.longitude,
                    unassigned_osm.way_id,
                    unassigned_osm.node_id,
                    unassigned_osm.bearing,
                    unassigned_osm.reverse_bearing,
                    unassigned_osm.fixed_street,
                    "stop_fixed_street",
                ]
                + stop_cols
            )
            .withColumn(
                "bearing_dir",
                get_closer_bearing(
                    F.col("stop_bearing"), F.col("bearing"), F.col("reverse_bearing")
                ),
            )
            .withColumn("use_bearing", lit(False))
        )

        osm_stop_signs_all = osm_stop_by_name.unionByName(osm_stop_signs)

        # Filter if street names are not the same and are both not empty
        osm_stop_signs_all = osm_stop_signs_all.withColumn(
            "equal_or_empty",
            equal_names_or_empty(F.col("stop_fixed_street"), F.col("fixed_street")),
        )
        osm_stop_signs_all = osm_stop_signs_all.filter(F.col("equal_or_empty") == True)
        osm_stop_signs_all = osm_stop_signs_all.drop("equal_or_empty")

        osm_stop_signs_all.write.mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(f"stopsigns.{self.city_name}_stop_signs_osm")

    def gen_non_stop_sign_osm(self):
        non_stop_sign_sdf_no_dir = spark.sql(
            f"""
                select
                    latitude,
                    longitude,
                    way_id,
                    node_id,
                    bearing,
                    reverse_bearing
                from stopsigns.{self.city_name}_intersections_stop_boundaries
                where struct(way_id, node_id) not in
                    (select way_id, node_id from stopsigns.{self.city_name}_stop_signs_osm)
            """
        )
        non_stop_sign_sdf_forward = non_stop_sign_sdf_no_dir.withColumn(
            "bearing_dir", lit(1)
        )
        non_stop_sign_sdf_reverse = non_stop_sign_sdf_no_dir.withColumn(
            "bearing_dir", lit(-1)
        )
        non_stop_sign_sdf = non_stop_sign_sdf_forward.unionByName(
            non_stop_sign_sdf_reverse
        )
        non_stop_sign_sdf.write.mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(f"stopsigns.{self.city_name}_non_stop_signs_osm")

    def get_clean_stop_signs(self):
        tn = f"stopsigns.{self.city_name}_stop_signs_osm"
        ss_sdf = spark.table(tn)
        ss_sdf = ss_sdf.withColumn(
            "distance",
            haversine_distance(
                F.col("latitude"),
                F.col("longitude"),
                F.col("stop_lat"),
                F.col("stop_lon"),
            ).cast("double"),
        )
        ss_sdf.createOrReplaceTempView("stop_signs")
        ss_sdf = spark.sql(
            f"""
            WITH best_match AS (
            SELECT
                way_id,
                bearing,
                stop_lat,
                stop_lon,
                MIN_BY(node_id, distance) AS best_node_id
            FROM stop_signs
            GROUP BY
                way_id,
                bearing,
                stop_lat,
                stop_lon
            )
            SELECT
            sso.latitude,
            sso.longitude,
            sso.way_id,
            sso.node_id,
            sso.bearing,
            sso.reverse_bearing,
            sso.bearing_dir,
            '{self.city_name}' AS city,
            IF(bm.way_id IS NOT NULL, 1, 0) AS has_stop_sign
            FROM {tn} sso
            LEFT JOIN best_match bm
            ON bm.way_id = sso.way_id
                AND bm.bearing = sso.bearing
                AND bm.stop_lat = sso.stop_lat
                AND bm.stop_lon = sso.stop_lon
                AND bm.best_node_id = sso.node_id
        """
        )
        return ss_sdf

    def get_non_stop_signs(self):
        tn = f"stopsigns.{self.city_name}_non_stop_signs_osm"
        nss_sdf = spark.sql(
            f"""
            SELECT
            sso.latitude,
            sso.longitude,
            sso.way_id,
            sso.node_id,
            sso.bearing,
            sso.reverse_bearing,
            sso.bearing_dir,
            '{self.city_name}' AS city,
            0 AS has_stop_sign

            FROM {tn} sso
            """
        )
        return nss_sdf

    def gen_city_stops(self):
        df = self.get_non_stop_signs()
        df = df.unionAll(self.get_clean_stop_signs())
        df.createOrReplaceTempView("ss_df")
        df = spark.sql(
            """
            SELECT
            latitude,
            longitude,
            way_id,
            node_id,
            bearing,
            reverse_bearing,
            bearing_dir,
            city,
            MAX(has_stop_sign) AS has_stop_sign

            FROM ss_df
            GROUP BY
            latitude,
            longitude,
            way_id,
            node_id,
            bearing,
            reverse_bearing,
            bearing_dir,
            city
        """
        )

        write_time = int(time.time())
        write_date = str(date.today())

        df.withColumn("date", F.lit(write_date)).withColumn(
            "ts", F.list(write_time)
        ).write.format("delta").mode("append").partitionBy("date").saveAsTable(
            "stopsigns.ground_truth_city_labels"
        )

    def run_all(self):
        self.gen_bbox_sdf()
        self.gen_exploded_sdf()
        self.gen_intersection_stop_boundaries()
        self.gen_stop_sign_osm()
        self.gen_non_stop_sign_osm()
        self.gen_city_stops()
