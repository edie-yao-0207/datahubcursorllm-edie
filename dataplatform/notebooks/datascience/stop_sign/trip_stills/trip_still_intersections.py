# Databricks notebook source
# Following install commands were commented out as they are no longer
# supported in UC clusters. Please ensure that this notebook is run
# in a cluster with the required libraries installed.
# dbutils.library.installPyPI("contextlib2")
# dbutils.library.installPyPI("scikit-image")
# dbutils.library.installPyPI("h3")
# dbutils.library.installPyPI("geopandas")
# dbutils.library.installPyPI("geojson")

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

from multiprocessing import cpu_count
from multiprocessing.dummy import Pool
import os

import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------


@udf("boolean")
def bearing_in_range(
    intsec_bearing, loc_bearing, intsec_bearing_max, intsec_bearing_min
):
    if intsec_bearing < -150 or intsec_bearing > 150:
        return loc_bearing <= intsec_bearing_max or loc_bearing >= intsec_bearing_min
    return loc_bearing >= intsec_bearing_min and loc_bearing <= intsec_bearing_max


def get_loc_bearing_sdf(loc_bearing_sdf, bearing_dir):
    loc_bearing_sdf = loc_bearing_sdf.withColumnRenamed("latitude", "intsec_lat")
    loc_bearing_sdf = loc_bearing_sdf.withColumnRenamed("longitude", "intsec_lon")
    loc_bearing_sdf = loc_bearing_sdf.withColumn("bearing_dir", lit(bearing_dir))
    return loc_bearing_sdf


# COMMAND ----------


def get_dist_from_int(int_lat, int_lon, trip_lat, trip_lon):
    return get_raw_distance(
        float(int_lat), float(int_lon), float(trip_lat), float(trip_lon)
    )


@udf(
    ArrayType(
        StructType(
            [
                StructField("s3url", StringType()),
                StructField("dbfs_path", StringType()),
                StructField("trip_still_lat", StringType()),
                StructField("trip_still_lon", StringType()),
                StructField("org_id", StringType()),
                StructField("date", StringType()),
            ]
        )
    )
)
def get_top_trip_stills(trip_still_list, int_lat, int_lon):
    CAP = 20
    if len(trip_still_list) <= CAP:
        return trip_still_list
    sorted_trip_list = sorted(
        trip_still_list,
        key=lambda x: get_raw_distance(
            int_lat, int_lon, x.trip_still_lat, x.trip_still_lon
        ),
    )
    return sorted_trip_list[:20]


# COMMAND ----------


class TripStillIntersections:
    def __init__(self, city_name):
        self.city_name = city_name

    def assign_to_ints(self, mode="overwrite", min_date_for_update="2017-01-01"):
        if mode == "ignore":
            return
        loc_sdf = spark.table(
            f"stopsigns.trip_stills_locations_{self.city_name}"
        ).dropna()
        # Filter by date
        loc_sdf = loc_sdf.filter(F.col("date") >= min_date_for_update)
        ints_sdf = spark.table(f"stopsigns.{self.city_name}_intersections_exploded_h3")
        cols_to_select = [
            loc_sdf.date,
            loc_sdf.org_id,
            loc_sdf.cm_device_id,
            loc_sdf.file_timestamp,
            loc_sdf.device_id,
            loc_sdf.uploaded_raw_s3url,
            loc_sdf.s3url,
            loc_sdf.trip_still_lat,
            loc_sdf.trip_still_lon,
            loc_sdf.hex_id_res_13,
            ints_sdf.way_id,
            "node_id",
            "bearing",
            "reverse_bearing",
            "latitude",
            "longitude",
        ]

        loc_forward_bearing = loc_sdf.join(
            ints_sdf,
            (ints_sdf.bounding_box_exploded_h3 == loc_sdf.hex_id_res_13)
            & bearing_in_range(
                ints_sdf.bearing,
                loc_sdf.loc_bearing,
                ints_sdf.bearing_max,
                ints_sdf.bearing_min,
            ),
        ).select(cols_to_select)
        loc_forward_bearing = get_loc_bearing_sdf(loc_forward_bearing, 1)

        loc_reverse_bearing = loc_sdf.join(
            ints_sdf,
            (ints_sdf.bounding_box_exploded_h3 == loc_sdf.hex_id_res_13)
            & bearing_in_range(
                ints_sdf.reverse_bearing,
                loc_sdf.loc_bearing,
                ints_sdf.reverse_bearing_max,
                ints_sdf.reverse_bearing_min,
            ),
        ).select(cols_to_select)
        loc_reverse_bearing = get_loc_bearing_sdf(loc_reverse_bearing, -1)

        loc_bearing = loc_forward_bearing.unionByName(loc_reverse_bearing)

        loc_bearing = loc_bearing.withColumn(
            "hex_id_res_3",
            lat_lng_to_h3_str_udf(col("trip_still_lat"), col("trip_still_lon"), lit(3)),
        )

        write_table_with_date_partition(
            loc_bearing,
            f"stopsigns.trip_stills_intersections_{self.city_name}",
            min_date_for_update=min_date_for_update,
        )

    def _keep_top_stills(self, trip_ints):
        trip_ints_grouped = trip_ints.groupBy(
            [
                "intsec_lat",
                "intsec_lon",
                "way_id",
                "node_id",
                "bearing_dir",
                "hex_id_res_3",
            ]
        ).agg(
            F.collect_list(
                F.struct(
                    F.col("s3url"),
                    F.col("dbfs_path"),
                    F.col("trip_still_lat"),
                    F.col("trip_still_lon"),
                    F.col("org_id"),
                    F.col("date"),
                )
            ).alias("trip_still_list")
        )
        trip_ints_top_stills = trip_ints_grouped.withColumn(
            "top_stills",
            get_top_trip_stills(
                F.col("trip_still_list"), F.col("intsec_lat"), F.col("intsec_lon")
            ),
        )
        trip_ints_top_stills = trip_ints_top_stills.withColumn(
            "exploded_trip_list", F.explode("top_stills")
        )
        trip_ints_top_stills = trip_ints_top_stills.withColumn(
            "dbfs_path", F.col("exploded_trip_list").dbfs_path
        )
        trip_ints_top_stills = trip_ints_top_stills.withColumn(
            "date", F.col("exploded_trip_list").date
        )
        return trip_ints_top_stills.select("dbfs_path", "date")

    def filter_urls(self, s3url_sdf):
        retention_sdf = spark.table("retentiondb_shards.retention_config").filter(
            F.col("data_type") == 4
        )
        retention_sdf = retention_sdf.select("org_id", "retain_days").dropDuplicates()
        s3url_sdf_retention = s3url_sdf.join(retention_sdf, "org_id", how="left")
        s3url_sdf_retention = s3url_sdf_retention.filter(
            (F.col("retain_days").isNull() == True)
        )  # 1 month buffer
        s3url_sdf_retention = s3url_sdf_retention.select("s3url")
        s3url_sdf = s3url_sdf_retention.withColumn("length", F.length(F.col("s3url")))
        s3url_sdf = s3url_sdf.withColumn(
            "dbfs_path",
            F.concat(
                F.lit("/dbfs/mnt"), F.col("s3url").substr(F.lit(5), F.col("length") - 4)
            ),
        )
        return s3url_sdf.select("s3url", "dbfs_path")

    def filter_predeleted_urls(self, trip_still_intersections_at_index, index):
        # We do have run filtering on all stills,
        # however we can at least ignore
        # stills we've already filtered out
        try:
            current_table = spark.table(
                f"stopsigns.trip_stills_intersections_paths_{self.city_name}_h3_res_3_{index}"
            ).select("org_id", "date", "s3url")
            max_old_date = current_table.agg({"date": "max"}).collect()[0][0]
            new_stills = trip_still_intersections_at_index.filter(
                F.col("date") > max_old_date
            )
            return new_stills.unionByName(current_table)
        except:
            return trip_still_intersections_at_index.select("org_id", "date", "s3url")

    # chunk by h3 res 3
    def gen_image_paths_df(self, mode="overwrite", min_date_for_update="2017-01-01"):
        if mode == "ignore":
            return
        trip_stills_intersections = spark.table(
            f"stopsigns.trip_stills_intersections_{self.city_name}"
        )
        h3_res_3_index_list = (
            trip_stills_intersections.select("hex_id_res_3")
            .dropDuplicates()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        h3_table_name = f"stopsigns.{self.city_name}_trip_still_image_df_index_tracker"
        finished_h3 = []
        if table_exists(h3_table_name):
            finished_h3 = (
                spark.table(h3_table_name)
                .select("h3_index")
                .rdd.flatMap(lambda x: x)
                .collect()
            )
        write_h3_table(h3_table_name, finished_h3)
        s3urls_filtered_all = []
        for h3_res_3_index in h3_res_3_index_list:
            if h3_res_3_index in finished_h3:
                continue
            print(f"Computing {h3_res_3_index}")
            trip_stills_intersections_at_index = trip_stills_intersections.filter(
                col("hex_id_res_3") == h3_res_3_index
            )
            print("Filtering...")
            trip_stills_predeleted_removed = self.filter_predeleted_urls(
                trip_stills_intersections_at_index.select("org_id", "date", "s3url"),
                h3_res_3_index,
            )
            # Filter urls that don't exist
            s3urls_filtered_sdf = self.filter_urls(trip_stills_predeleted_removed)
            # Group top 20 stills
            print("Keeping Top 20 Stills Per Intersection...")
            filtered_ints = s3urls_filtered_sdf.join(
                trip_stills_intersections_at_index, "s3url"
            )
            top_stills = self._keep_top_stills(filtered_ints)
            print("Writing table...")
            write_table_with_date_partition(
                top_stills,
                f"stopsigns.trip_stills_intersections_paths_{self.city_name}_h3_res_3_{h3_res_3_index}",
                min_date_for_update=min_date_for_update,
            )
            finished_h3 = finished_h3 + [h3_res_3_index]
            write_h3_table(h3_table_name, finished_h3)

        print("Done!")
        drop_h3_table(h3_table_name)

    def update_intersections(self, s3urls_filtered, intersections_sdf):
        s3urls_filtered_sdf = spark.createDataFrame(
            [Row(s3url=s3url) for s3url in s3urls_filtered]
        )
        filtered_intersections = intersections_sdf.join(s3urls_filtered_sdf, "s3")
        write_table_no_partition(
            filtered_intersections,
            f"stopsigns.trip_stills_intersections_{self.city_name}",
        )


# COMMAND ----------


# COMMAND ----------
