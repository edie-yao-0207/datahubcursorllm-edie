# Databricks notebook source

# COMMAND ----------

# MAGIC %run "/backend/datascience/stop_sign/utils"

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.queryWatchdog.maxQueryTasks = 180000

# COMMAND ----------

import os

from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------


def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            return False
        else:
            raise


@udf
def gen_histogram(int_info, training, prefix):
    telemetry_data = int_info.telemetry_data

    folder_prefix = prefix
    if training:
        folder_prefix = prefix + "/non_stop_sign"
        if int_info.label == 1:
            folder_prefix = prefix + "/stop_sign"

    fname = "{}/instersection_way_id_{}_node_id_{}_lat_{}_lon_{}_bearing_{}_dir_{}_num_points_{}".format(
        folder_prefix,
        int_info.way_id,
        int_info.node_id,
        int_info.intsec_lat,
        int_info.intsec_lon,
        int_info.bearing,
        int_info.bearing_dir,
        len(telemetry_data.speeds),
    )
    fname = fname.replace(".", "_")

    cube_root = [np.cbrt(speed) for speed in telemetry_data.speeds]
    min_root = min(cube_root)
    max_root = max(cube_root)
    min_max_norm_speed = [
        (c_r_speed - min_root) / (max_root - min_root) for c_r_speed in cube_root
    ]
    for norm_val in min_max_norm_speed:
        if math.isnan(norm_val) or math.isinf(norm_val):
            return ""
    plt.gca().xaxis.set_major_locator(plt.NullLocator())
    plt.gca().yaxis.set_major_locator(plt.NullLocator())
    kdeplot_test = sns.kdeplot(
        telemetry_data.distances, min_max_norm_speed, shade=True, color="b"
    )
    fig = kdeplot_test.get_figure()
    fig.savefig(fname, bbox_inches="tight", pad_inches=0)
    plt.close(fig)
    return fname


# COMMAND ----------


@udf
def gen_s3_img_link(path, prefix):
    path_prefix = f"https://s3.internal.samsara.com/s3/samsara-databricks-playground/stop_sign_detection/{prefix}/"
    fname = path.split("/")[-1]
    return path_prefix + fname


@udf
def extract_lat(path, lat_index):
    fname = path.split("/")[-1]
    fname_split = fname.split("_")
    lat_int = fname_split[lat_index]
    lat_dec = fname_split[lat_index + 1]
    lat_str = lat_int + "." + lat_dec
    return float(lat_str)


@udf
def extract_trace_device_count(path):
    fname = path.split("/")[-1]
    fname_split = fname.split("_")
    num = fname_split[-1].split(".")[0]
    return num


@udf
def extract_trace_count(path):
    fname = path.split("/")[-1]
    num = fname.split("_")[-5]
    return num


@udf
def extract_lon(path, lon_index):
    fname = path.split("/")[-1]
    fname_split = fname.split("_")
    lon_int = fname_split[lon_index]
    lon_dec = fname_split[lon_index + 1]
    lon_str = lon_int + "." + lon_dec
    return float(lon_str)


@udf
def extract_way_id(path, way_index):
    fname = path.split("/")[-1]
    fname_split = fname.split("_")
    return fname_split[way_index]


@udf
def extract_node_id(path, node_index):
    fname = path.split("/")[-1]
    fname_split = fname.split("_")
    return fname_split[node_index]


@udf
def extract_direction(path, dir_index):
    fname = path.split("/")[-1]
    fname_split = fname.split("_")
    return fname_split[dir_index]


@udf
def extract_bearing(path, bearing_index):
    fname = path.split("/")[-1]
    fname_split = fname.split("_")
    bearing_int = fname_split[bearing_index]
    bearing_dec = fname_split[bearing_index + 1]
    bearing_str = bearing_int + "." + bearing_dec
    return float(bearing_str)


@udf
def gmaps_link(lat, lon):
    return f"https://maps.google.com/?q={lat},{lon}"


@udf
def osm_way_link(way_id):
    return f"https://www.openstreetmap.org/way/{way_id}"


def get_inference_df(
    inference_results_df,
    prefix,
    lat_index,
    lon_index,
    way_index,
    node_index,
    bearing_index,
    dir_index,
):
    inference_df = inference_results_df.withColumn(
        "<img>", gen_s3_img_link(F.col("path"), F.lit(prefix))
    )
    inference_df = inference_df.withColumn(
        "lat", extract_lat(F.col("path"), F.lit(lat_index)).cast("double")
    )
    inference_df = inference_df.withColumn(
        "lon", extract_lon(F.col("path"), F.lit(lon_index)).cast("double")
    )
    inference_df = inference_df.withColumn(
        "way_id", extract_way_id(F.col("path"), F.lit(way_index))
    )
    inference_df = inference_df.withColumn(
        "node_id", extract_node_id(F.col("path"), F.lit(node_index))
    )
    inference_df = inference_df.withColumn(
        "gmaps_link", gmaps_link(F.col("lat"), F.col("lon"))
    )
    inference_df = inference_df.withColumn(
        "osm_way_link", osm_way_link(F.col("way_id"))
    )
    inference_df = inference_df.withColumn(
        "trace_count", extract_trace_count(F.col("path"))
    )
    inference_df = inference_df.withColumn(
        "trace_device_count", extract_trace_device_count(F.col("path"))
    )
    inference_df = inference_df.withColumn(
        "bearing", extract_bearing(F.col("path"), F.lit(bearing_index))
    )
    inference_df = inference_df.withColumn(
        "bearing_dir", extract_direction(F.col("path"), F.lit(dir_index))
    )
    inference_df = inference_df.withColumn(
        "hex_id_res_3", lat_lng_to_h3_str_udf(col("lat"), col("lon"), lit(3))
    )
    return inference_df


# COMMAND ----------

PREFIX = "dbfs:/mnt/samsara-databricks-playground/stop_sign_detection"


class RegionTelemetryGenerator:
    # We get all intersections in between a NW lat/lon and a SE lat/lon (essentially defining a bounding box)
    def __init__(
        self,
        nw_lat,
        nw_lon,
        se_lat,
        se_lon,
        region_name,
        hist_limit=100,
    ):
        self.nw_lat = nw_lat
        self.nw_lon = nw_lon
        self.se_lat = se_lat
        self.se_lon = se_lon
        self.region_name = region_name
        self.table_name = f"stopsigns.{self.region_name}_intersections"
        self.exploded_h3_table_name = f"{self.table_name}_exploded_h3"
        self.loc_data_table = f"stopsigns.{self.region_name}_loc_data"
        self.telemetry_data_table = f"stopsigns.{self.region_name}_telemetry_data"
        self.s3_folder_name = f"{PREFIX}/{self.region_name}_histograms_{hist_limit}"
        self.hist_limit = hist_limit

    def _get_intersections_from_us_osm(self):
        us_osm = spark.table("stopsigns.osm_us_intersections")
        us_osm_box = us_osm.filter(
            (us_osm.latitude.between(self.se_lat, self.nw_lat))
            & (us_osm.longitude.between(self.nw_lon, self.se_lon))
        )
        return us_osm_box

    def _filter_complex_intersections(self, unfiltered_ints):
        # Filter out any intersections with >= 5 ways
        return unfiltered_ints.filter(size(col("intersecting_ways")) < 5)

    def _explode_intersections_way_node_level(self, intersections_node_level):
        intersections_way = intersections_node_level.withColumn(
            "ways_exploded", explode("intersecting_ways")
        )
        intersections_way = intersections_way.withColumn(
            "bearing", intersections_way.ways_exploded.bearing
        )
        intersections_way = intersections_way.withColumn(
            "reverse_bearing", intersections_way.ways_exploded.reverse_bearing
        )
        intersections_way = intersections_way.withColumn(
            "way_id", intersections_way.ways_exploded.way_id
        )
        intersections_way = intersections_way.withColumn(
            "highway_tag_value", intersections_way.ways_exploded.highway_tag_value
        )
        intersections_way = intersections_way.withColumn(
            "fixed_street", intersections_way.ways_exploded.fixed_street
        )
        return intersections_way

    def _gen_bounding_box_and_bearings(self, intersections_sdf):
        return gen_bounding_box_and_bearings(intersections_sdf)

    def generate_intersections(self, mode="overwrite"):
        if mode == "ignore":
            return
        raw_intersections = self._get_intersections_from_us_osm()
        filtered_intersections = self._filter_complex_intersections(raw_intersections)
        exploded_intersections = self._explode_intersections_way_node_level(
            filtered_intersections
        )
        intersections_bbox = self._gen_bounding_box_and_bearings(exploded_intersections)
        write_table_no_partition(intersections_bbox, self.table_name, mode="overwrite")
        intersections_bbox_exploded_h3 = intersections_bbox.withColumn(
            "bounding_box_exploded_h3", explode("bounding_box_h3")
        )
        write_table_no_partition(
            intersections_bbox_exploded_h3,
            self.exploded_h3_table_name,
            mode="overwrite",
        )

    def _gen_loc_bearing(self, loc_sdf):
        windowSpec = Window.partitionBy("device_id").orderBy("time")
        loc_sdf = loc_sdf.withColumn(
            "loc_bearing",
            calculate_bearing(
                col("lng"),
                col("lat"),
                lead("lng").over(windowSpec),
                lead("lat").over(windowSpec),
            ),
        )
        loc_sdf = loc_sdf.dropna()
        return loc_sdf

    def _partition_telemetry_data_to_intersections(
        self, loc_sdf, min_date_for_update="2017-01-01"
    ):
        # filter location data points by date
        loc_sdf = loc_sdf.filter(F.col("date") >= min_date_for_update)
        intersections_sdf = spark.table(self.exploded_h3_table_name)
        intersections_sdf = intersections_sdf.withColumnRenamed(
            "latitude", "intsec_lat"
        )
        intersections_sdf = intersections_sdf.withColumnRenamed(
            "longitude", "intsec_lon"
        )
        cols_to_select = [
            "date",
            "fixed_street",
            "speed_meters_per_second",
            "device_id",
            "time",
            "lat",
            "lng",
            "intsec_lat",
            "intsec_lon",
            "min_lat",
            "min_lng",
            "bearing",
            "reverse_bearing",
            "bounding_box_exploded_h3",
            "loc_bearing",
            intersections_sdf.way_id,
            "node_id",
            "hex_id_res_3",
        ]
        #     cols_to_select = cols_to_select + ["date"]
        loc_int_forward_bearing = (
            loc_sdf.join(
                intersections_sdf,
                (intersections_sdf.bounding_box_exploded_h3 == loc_sdf.hex_id)
                & bearing_in_range(
                    intersections_sdf.bearing,
                    loc_sdf.loc_bearing,
                    intersections_sdf.bearing_max,
                    intersections_sdf.bearing_min,
                ),
            )
            .select(cols_to_select)
            .withColumn("bearing_dir", lit(1))
        )
        loc_int_reverse_bearing = (
            loc_sdf.join(
                intersections_sdf,
                (intersections_sdf.bounding_box_exploded_h3 == loc_sdf.hex_id)
                & bearing_in_range(
                    intersections_sdf.reverse_bearing,
                    loc_sdf.loc_bearing,
                    intersections_sdf.reverse_bearing_max,
                    intersections_sdf.reverse_bearing_min,
                ),
            )
            .select(cols_to_select)
            .withColumn("bearing_dir", lit(-1))
        )
        return loc_int_forward_bearing.unionByName(loc_int_reverse_bearing)

    def _get_distance(self, loc_bearing_sdf):
        return loc_bearing_sdf.withColumn(
            "distance",
            haversine_distance(
                col("min_lat"), col("min_lng"), col("lat"), col("lng")
            ).cast("double"),
        )

    def generate_telemetry_data(
        self, mode="overwrite", min_date_for_update="2017-01-01"
    ):
        if mode == "ignore":
            return
        loc_sdf = spark.table(self.loc_data_table)
        loc_int_bearing = self._partition_telemetry_data_to_intersections(
            loc_sdf, min_date_for_update=min_date_for_update
        )
        loc_distance = self._get_distance(loc_int_bearing)
        write_table_with_date_partition(
            loc_distance,
            self.telemetry_data_table,
            min_date_for_update=min_date_for_update,
        )

    def label_telemetry_data(self, mode="overwrite"):
        stop_signs = spark.table(f"stopsigns.{self.region_name}_stop_signs_osm")
        non_stop_signs = spark.table(f"stopsigns.{self.region_name}_non_stop_signs_osm")
        telemetry = spark.table(self.telemetry_data_table)
        cols_to_select = [
            telemetry.fixed_street,
            telemetry.speed_meters_per_second,
            telemetry.distance,
            telemetry.device_id,
            telemetry.time,
            telemetry.lat,
            telemetry.lng,
            telemetry.intsec_lat,
            telemetry.intsec_lon,
            telemetry.min_lat,
            telemetry.min_lng,
            telemetry.bearing,
            telemetry.reverse_bearing,
            telemetry.bounding_box_exploded_h3,
            telemetry.loc_bearing,
            telemetry.date,
            telemetry.way_id,
            telemetry.node_id,
            telemetry.bearing_dir,
        ]
        telemetry_stop_signs = (
            telemetry.join(stop_signs, ["way_id", "node_id", "bearing_dir"])
            .select(cols_to_select)
            .withColumn("label", lit(1))
        )
        telemetry_non_stop_signs = (
            telemetry.join(non_stop_signs, ["way_id", "node_id", "bearing_dir"])
            .select(cols_to_select)
            .withColumn("label", lit(0))
        )
        telemetry_labeled = telemetry_stop_signs.unionByName(telemetry_non_stop_signs)
        write_table_no_partition(
            telemetry_labeled,
            f"stopsigns.{self.region_name}_telemetry_labeled",
            mode=mode,
        )

    def generate_location_data(
        self, mode="overwrite", min_date_for_update="2017-01-01"
    ):
        if mode == "ignore":
            return
        loc_query = f"""
      select
        loc.value.way_id,
        device_id,
        date,
        time,
        value.latitude as lat,
        value.longitude as lng,
        coalesce(value.ecu_speed_meters_per_second, value.gps_speed_meters_per_second) as speed_meters_per_second
      from kinesisstats.location loc
      join clouddb.organizations orgs
        on orgs.id = loc.org_id
        and orgs.internal_type <> 1
        and orgs.quarantine_enabled <> 1
      join productsdb.devices devices
        on loc.device_id = devices.id
        and devices.product_id in (7, 17, 24)
      where
        loc.value.has_fix = true and
        loc.date >= '2015-01-01' and
        (loc.value.latitude between {self.se_lat} and {self.nw_lat} and
        loc.value.longitude between {self.nw_lon} and {self.se_lon})
    """
        loc_sdf = spark.sql(loc_query)
        # filter location datapoints by date
        loc_sdf = loc_sdf.filter(F.col("date") >= min_date_for_update)
        loc_sdf = loc_sdf.withColumn(
            "hex_id", lat_lng_to_h3_str_udf(col("lat"), col("lng"), lit(13))
        )
        loc_sdf = loc_sdf.withColumn(
            "hex_id_res_3", lat_lng_to_h3_str_udf(col("lat"), col("lng"), lit(3))
        )
        loc_sdf = self._gen_loc_bearing(loc_sdf)
        write_table_with_date_partition(
            loc_sdf, self.loc_data_table, min_date_for_update=min_date_for_update
        )

    def _create_s3_folder(self, folder_name):
        if not file_exists(folder_name):
            dbutils.fs.mkdirs(folder_name)

    def _clear_old_histograms(self):
        if file_exists(self.s3_folder_name):
            dbutils.fs.rm(self.s3_folder_name, recurse=True)

    def gen_histograms(self, data_sdf, prefix, hist_limit, training=False):
        group_cols = [
            "way_id",
            "node_id",
            "intsec_lat",
            "intsec_lon",
            "bearing",
            "bearing_dir",
        ]
        print(f"Training: {training}")
        if training:
            group_cols = group_cols + ["label"]
            hist_struct = struct(
                col("way_id"),
                col("node_id"),
                col("intsec_lat"),
                col("intsec_lon"),
                col("bearing"),
                col("bearing_dir"),
                col("telemetry_data"),
                col("label"),
            )

            # stop sign data
            stop_sign_data = (
                data_sdf.filter(col("label") == 1)
                .groupBy(group_cols)
                .agg(
                    struct(
                        collect_list(col("speed_meters_per_second")).alias("speeds"),
                        collect_list(col("distance")).alias("distances"),
                    ).alias("telemetry_data")
                )
            )
            stop_sign_data = stop_sign_data.filter(
                F.size(col("telemetry_data").speeds) >= hist_limit
            )
            stop_sign_lim = stop_sign_data.count()

            # non stop sign
            non_stop_sign_data_whole = (
                data_sdf.filter(col("label") == 0)
                .groupBy(group_cols)
                .agg(
                    struct(
                        collect_list(col("speed_meters_per_second")).alias("speeds"),
                        collect_list(col("distance")).alias("distances"),
                    ).alias("telemetry_data")
                )
            )
            non_stop_sign_data_whole = non_stop_sign_data_whole.filter(
                F.size(col("telemetry_data").speeds) >= hist_limit
            )
            lim_percent = stop_sign_lim / non_stop_sign_data_whole.count()
            non_stop_sign_data, _ = non_stop_sign_data_whole.randomSplit(
                [lim_percent, 1 - lim_percent]
            )

            # gen histograms
            stop_sign_data = stop_sign_data.withColumn(
                "histogram_link", gen_histogram(hist_struct, lit(training), lit(prefix))
            )
            non_stop_sign_data = non_stop_sign_data.withColumn(
                "histogram_link", gen_histogram(hist_struct, lit(training), lit(prefix))
            )

            print("Generating stop sign histograms...")
            stop_sign_data.rdd.count()
            print("Generating non stop sign histograms...")
            non_stop_sign_data.rdd.count()

            return

        data_sdf = data_sdf.groupBy(group_cols).agg(
            struct(
                collect_list(col("speed_meters_per_second")).alias("speeds"),
                collect_list(col("distance")).alias("distances"),
            ).alias("telemetry_data")
        )
        data_sdf = data_sdf.filter(size(col("telemetry_data").speeds) >= hist_limit)

        hist_struct = struct(
            col("way_id"),
            col("node_id"),
            col("intsec_lat"),
            col("intsec_lon"),
            col("bearing"),
            col("bearing_dir"),
            col("telemetry_data"),
        )
        data_sdf = data_sdf.withColumn(
            "histogram_link", gen_histogram(hist_struct, lit(training), lit(prefix))
        )

        print("Generating histograms...")
        data_sdf.rdd.count()

        return

    def generate_histograms(
        self,
        training=False,
        clear_histograms=True,
        mode="overwrite",
    ):
        if mode == "ignore":
            return
        # TODO: walk directory and create dataframe of way id, node id & dir that have already been generated, then ignore those.
        if training:
            self.s3_folder_name = f"{PREFIX}/training_dataset"
        if clear_histograms:
            print("Clearing histograms...")
            self._clear_old_histograms()
            print("Creating folder...")
            self._create_s3_folder(self.s3_folder_name)
            if training:
                self._create_s3_folder(self.s3_folder_name + "/stop_sign/")
                self._create_s3_folder(self.s3_folder_name + "/non_stop_sign/")

        telem_sdf_name = self.telemetry_data_table
        if training:
            telem_sdf_name = f"stopsigns.{self.region_name}_telemetry_labeled"

        data_sdf = spark.table(telem_sdf_name)
        print("Generating...")
        hist_limit = self.hist_limit
        if training:
            hist_limit = 1000

        h3_res_3_index_list = (
            data_sdf.select("hex_id_res_3")
            .dropDuplicates()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        print(h3_res_3_index_list)
        h3_table_name = f"stopsigns.{self.region_name}_gen_histograms_index_tracker"
        finished_h3 = []
        if table_exists(h3_table_name):
            finished_h3 = (
                spark.table(h3_table_name)
                .select("h3_index")
                .rdd.flatMap(lambda x: x)
                .collect()
            )
        write_h3_table(h3_table_name, finished_h3)
        for h3_res_3_index in h3_res_3_index_list:
            if h3_res_3_index in finished_h3:
                continue
            self._create_s3_folder(self.s3_folder_name + f"/{h3_res_3_index}/")
            print(f"Generating for {h3_res_3_index}")
            hist_folder_prefix = (
                "/dbfs" + self.s3_folder_name.split(":")[1] + f"/{h3_res_3_index}"
            )
            filtered_sdf = data_sdf.filter(col("hex_id_res_3") == h3_res_3_index)
            self.gen_histograms(
                filtered_sdf, hist_folder_prefix, hist_limit, training=training
            )
            finished_h3 = finished_h3 + [h3_res_3_index]
            write_h3_table(h3_table_name, finished_h3)
            print("Done")
        drop_h3_table(h3_table_name)

    def get_image_df(self, image_paths):
        schema = StructType([StructField("path", StringType(), True)])
        image_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        if len(image_paths):
            image_df = spark.createDataFrame(
                map(lambda path: (path,), image_paths), ["path"]
            )
        return image_df

    def create_inference_table(self, mode="overwrite"):
        if mode == "ignore":
            return
        telem_sdf_name = self.telemetry_data_table
        data_sdf = spark.table(telem_sdf_name)
        h3_res_3_index_list = (
            data_sdf.select("hex_id_res_3")
            .dropDuplicates()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        h3_table_name = (
            f"stopsigns.{self.region_name}_histogram_inference_table_index_tracker"
        )
        finished_h3 = []
        if table_exists(h3_table_name):
            finished_h3 = (
                spark.table(h3_table_name)
                .select("h3_index")
                .rdd.flatMap(lambda x: x)
                .collect()
            )
        write_h3_table(h3_table_name, finished_h3)
        for h3_res_3_index in h3_res_3_index_list:
            if h3_res_3_index in finished_h3:
                continue
            image_dir = f"/dbfs/mnt/samsara-databricks-playground/stop_sign_detection/{self.region_name}_histograms_{self.hist_limit}/{h3_res_3_index}/"
            image_paths = [
                os.path.join(dp, f)
                for dp, dn, filenames in os.walk(image_dir)
                for f in filenames
                if os.path.splitext(f)[1] == ".png"
            ]
            image_df = self.get_image_df(image_paths)
            image_df = get_inference_df(
                image_df,
                f"{self.region_name}_histograms_{self.hist_limit}",
                8,
                11,
                3,
                6,
                14,
                17,
            )
            write_table_no_partition(
                image_df,
                f"stopsigns.{self.region_name}_histogram_paths_{self.hist_limit}_{h3_res_3_index}",
                mode=mode,
            )
            finished_h3 = finished_h3 + [h3_res_3_index]
            write_h3_table(h3_table_name, finished_h3)
        drop_h3_table(h3_table_name)

    def create_agg_speed_table(self, mode="overwrite"):
        if mode == "ignore":
            print("Not building agg speed table for", self.region_name)
            return
        tn = f"stopsigns.{self.region_name}_agg_speed_data"
        print(f"Writing agg speed data to {tn}...")
        df = spark.sql(self.create_agg_speed_query())
        write_table_h3_res_3_partition(df, tn, mode)
        print(f"...done writing data")

    def create_agg_speed_query(self):
        first_aggs = ["min", "avg", "p50", "max"]
        agg_query = ""
        for fa in first_aggs:
            agg_query += f"""MIN({fa}_mps) min_{fa}_mps,
            MAX({fa}_mps) max_{fa}_mps,
            COALESCE(stddev({fa}_mps), 0) stddev_{fa}_mps,
            AVG({fa}_mps) mean_{fa}_mps,
            APPROX_PERCENTILE({fa}_mps, 0.25) p25_{fa}_mps,
            APPROX_PERCENTILE({fa}_mps, 0.5) p50_{fa}_mps,
            APPROX_PERCENTILE({fa}_mps, 0.75) p75_{fa}_mps,
            APPROX_PERCENTILE({fa}_mps, 0.9) p90_{fa}_mps,
            APPROX_PERCENTILE({fa}_mps, 0.95) p95_{fa}_mps,"""
        query = f"""
            WITH agg_speed AS (
                SELECT
                    node_id,
                    way_id,
                    bearing_dir,
                    intsec_lat AS latitude,
                    intsec_lon AS longitude,
                    device_id,
                    hex_id_res_3,
                    date,
                    MIN(speed_meters_per_second) min_mps,
                    AVG(speed_meters_per_second) avg_mps,
                    APPROX_PERCENTILE(speed_meters_per_second, 0.5) AS p50_mps,
                    MAX(speed_meters_per_second) AS max_mps
                FROM stopsigns.{self.region_name}_telemetry_data
                GROUP BY
                    node_id,
                    way_id,
                    bearing_dir,
                    intsec_lat,
                    intsec_lon,
                    device_id,
                    hex_id_res_3,
                    date
            )
            SELECT
            node_id,
            way_id,
            bearing_dir,
            latitude,
            longitude,
            hex_id_res_3,
            {agg_query}
            COUNT(1) AS n_obs
            FROM agg_speed
            GROUP BY
            node_id,
            way_id,
            bearing_dir,
            latitude,
            longitude,
            hex_id_res_3
        """
        return query

    def create_osm_way_tag_table(self, mode="overwrite"):
        if mode == "ignore":
            print("Not building way_tags for", self.region_name)
            return
        tablename = f"stopsigns.{self.region_name}_way_tags"
        print(f"Writing way tag data to {tablename}...")
        tag_query = f"""
        WITH tags_raw AS (
            SELECT
            DISTINCT
            way_id,
            region_name,
            n.latitude,
            n.longitude,
            EXPLODE(tags) as tag
            FROM stopsigns.osm_us_way_nodes n
            JOIN stopsigns.region_bounds rb
                ON n.latitude >= rb.se_lat and n.latitude < rb.nw_lat
                    AND n.longitude < rb.se_lon and n.longitude >= rb.nw_lon
            JOIN stopsigns.osm_us_intersections oui
                ON oui.node_id = n.node_id
            WHERE rb.region_name = '{self.region_name}'
        )
        SELECT DISTINCT
            way_id,
            region_name,
            latitude,
            longitude,
            tag['value_decoded'] as highway_tag
        FROM tags_raw
        WHERE tag['key_decoded'] = 'highway'
        """
        tag_df = spark.sql(tag_query)
        tag_df = tag_df.withColumn(
            "hex_id_res_3",
            lat_lng_to_h3_str_udf(F.col("latitude"), F.col("longitude"), F.lit(3)),
        )
        tag_df = tag_df.select(
            ["way_id", "region_name", "hex_id_res_3", "highway_tag"]
        ).distinct()
        write_table_h3_res_3_partition(tag_df, tablename)
        print("...done")


# COMMAND ----------
