# Databricks notebook source
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.util import MLUtils
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    StringType,
    StructField,
    StructType,
)
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import precision_recall_curve
from sklearn.model_selection import train_test_split

# COMMAND ----------

# MAGIC %run backend/datascience/stop_sign/utils

# COMMAND ----------


@udf("float")
def array_mean(trip_still_preds):
    if trip_still_preds is None:
        return None
    return float(np.mean([pred.stop_sign_confidence for pred in trip_still_preds]))


@udf("float")
def max_pred(trip_still_preds):
    if trip_still_preds is None:
        return None
    max_trip_still_pred = max(
        [t_pred.stop_sign_confidence for t_pred in trip_still_preds]
    )
    return max_trip_still_pred


@udf("int")
def get_num_confident_preds(trip_still_preds):
    if trip_still_preds is None:
        return None
    num_above = 0
    for pred in trip_still_preds:
        if pred is None:
            return None
        if float(pred.stop_sign_confidence) >= 0.9:
            num_above += 1
    return num_above


@udf("int")
def get_num_top_right_bbox(trip_still_preds):
    if trip_still_preds is None:
        return None
    return len(
        [
            pred
            for pred in trip_still_preds
            if pred.stop_sign_bbox[0] >= 0.5 and pred.stop_sign_bbox[1] >= 0.5
        ]
    )


@udf("float")
def get_avg_bbox_size(trip_still_preds):
    if trip_still_preds is None:
        return None

    return sum([get_bbox_area(pred.stop_sign_bbox) for pred in trip_still_preds]) / len(
        trip_still_preds
    )


@udf("float")
def get_max_bbox_size(trip_still_preds):
    if trip_still_preds is None:
        return None

    return max([get_bbox_area(pred.stop_sign_bbox) for pred in trip_still_preds])


# COMMAND ----------


class CombineData:
    def __init__(self, region_name, gbdt_model, hist_limit=100):
        self.region_name = region_name
        self.hist_limit = hist_limit
        self.model = gbdt_model

    def add_ground_truth(self, ints_sdf, ground_truth_cities):
        cities_stop_sign_sdf = spark.table(
            f"stopsigns.{ground_truth_cities[0]}_stop_signs_osm"
        )
        cities_non_stop_sign_sdf = spark.table(
            f"stopsigns.{ground_truth_cities[0]}_non_stop_signs_osm"
        )
        for i in range(1, len(ground_truth_cities)):
            # stop signs
            city_stop_sign_sdf = spark.table(
                f"stopsigns.{ground_truth_cities[i]}_stop_signs_osm"
            )
            cities_stop_sign_sdf = cities_stop_sign_sdf.unionByName(city_stop_sign_sdf)
            # non stops signs
            city_non_stop_sign_sdf = spark.table(
                f"stopsigns.{ground_truth_cities[i]}_non_stop_signs_osm"
            )
            cities_non_stop_sign_sdf = cities_non_stop_sign_sdf.unionByName(
                city_non_stop_sign_sdf
            )

        cities_stop_sign_sdf = cities_stop_sign_sdf.select(
            "way_id", "node_id", "bearing_dir"
        ).withColumn("is_ground_truth", F.lit(1))
        cities_non_stop_sign_sdf = cities_non_stop_sign_sdf.select(
            "way_id", "node_id", "bearing_dir"
        ).withColumn("is_ground_truth", F.lit(0))
        cities_sdf = cities_stop_sign_sdf.unionByName(cities_non_stop_sign_sdf)
        return ints_sdf.join(
            cities_sdf, ["way_id", "node_id", "bearing_dir"], how="left"
        )

    def process_dataset(self, dataset):
        dataset_avg = dataset.withColumn(
            "avg_trip_still_pred", array_mean(F.col("trip_still_preds"))
        )
        dataset_max = dataset_avg.withColumn(
            "max_trip_still_pred", max_pred(F.col("trip_still_preds"))
        )
        dataset_num_confident = dataset_max.withColumn(
            "num_confident_trip_still_pred",
            get_num_confident_preds(F.col("trip_still_preds")),
        )
        dataset_avg_bbox_size = dataset_num_confident.withColumn(
            "avg_bbox_size",
            get_avg_bbox_size(F.col("trip_still_preds")),
        )
        dataset_max_bbox_size = dataset_avg_bbox_size.withColumn(
            "max_bbox_size",
            get_max_bbox_size(F.col("trip_still_preds")),
        )
        dataset_processed = dataset_max_bbox_size.withColumn(
            "num_top_right_trip_still",
            get_num_top_right_bbox(F.col("trip_still_preds")),
        )  # num boxes in top right
        dataset_processed = dataset_processed.drop("trip_still_preds")
        dataset_processed = dataset_processed.withColumn(
            "trace_count", F.col("trace_count").cast("integer")
        )
        dataset_processed = dataset_processed.withColumnRenamed(
            "is_ground_truth", "label"
        )
        dataset_processed = dataset_processed.withColumn(
            "label", F.col("label").cast("double")
        )
        dataset_processed = dataset_processed.fillna(
            -1,
            subset=[
                "telemetry_conf",
                "trace_count",
                "avg_trip_still_pred",
                "max_trip_still_pred",
                "num_confident_trip_still_pred",
                "num_top_right_trip_still",
            ],
        )

        pred_cols = [
            "way_id",
            "node_id",
            "bearing_dir",
            "latitude",
            "longitude",
            "label",
        ] + self.model.features

        dataset_processed = dataset_processed.select(*pred_cols)

        return dataset_processed.toPandas()

    def get_preds(self, df):
        processed_df = self.process_dataset(df)
        data_X = processed_df[self.model.features]
        # If NA at this point indicates we don't have
        #  speed data so we should infer 0 (no stop)
        #  All of the data in our training sample had
        #  Some amount of speed data
        na_rows = data_X.isna().any(axis=1) | (
            (data_X.telemetry_conf == -1) & (data_X.avg_trip_still_pred == -1)
        )
        processed_df["prediction"] = 0.0
        if processed_df.loc[~na_rows, "prediction"].shape[0] > 0:
            processed_df.loc[~na_rows, "prediction"] = self.model.predict(
                data_X[~na_rows]
            )

        pred_df = spark.createDataFrame(processed_df).select(
            "way_id", "node_id", "bearing_dir", "latitude", "longitude", "prediction"
        )
        df_with_pred = df.join(
            pred_df, ["way_id", "node_id", "bearing_dir", "latitude", "longitude"]
        )
        df_with_pred = df_with_pred.withColumn(
            "combined_prediction",
            F.when(
                F.col("is_ground_truth").isNotNull(), F.col("is_ground_truth")
            ).otherwise(F.col("prediction")),
        )
        df_with_pred = df_with_pred.drop("prediction")
        return df_with_pred

    def get_telem_table(self, table_name):
        try:
            return spark.table(table_name)
        except:
            schema = StructType(
                [
                    StructField("way_id", StringType(), True),
                    StructField("node_id", StringType(), True),
                    StructField("bearing_dir", StringType(), True),
                    StructField("probability_of_stop_sign", FloatType(), True),
                    StructField("<img>", StringType(), True),
                    StructField("trace_count", StringType(), True),
                ]
            )
            return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    def get_trip_stills_table(self, table_name):
        try:
            return spark.table(table_name)
        except:
            schema = StructType(
                [
                    StructField("way_id", StringType(), True),
                    StructField("node_id", StringType(), True),
                    StructField("bearing_dir", StringType(), True),
                    StructField("stop_sign_confidence", FloatType(), True),
                    StructField("stop_sign_bbox", ArrayType(FloatType()), True),
                    StructField("s3url", StringType(), True),
                ]
            )
            return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    def combine(self, ground_truth_cities=[], mode="overwrite"):
        if mode == "ignore":
            return
        h3_res_3_index_list = (
            spark.table(f"stopsigns.{self.region_name}_telemetry_data")
            .select("hex_id_res_3")
            .dropDuplicates()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        print(h3_res_3_index_list)
        h3_table_name = f"stopsigns.{self.region_name}_combine_data_index_tracker"
        finished_h3 = []
        if table_exists(h3_table_name):
            finished_h3 = (
                spark.table(h3_table_name)
                .select("h3_index")
                .rdd.flatMap(lambda x: x)
                .collect()
            )
        write_h3_table(h3_table_name, finished_h3)
        all_ints_raw = spark.table(
            f"stopsigns.{self.region_name}_intersections"
        ).select(
            "way_id", "node_id", "latitude", "longitude", "bearing", "reverse_bearing"
        )
        all_ints_forward = all_ints_raw.withColumn("bearing_dir", F.lit(1))
        all_ints_reverse = all_ints_raw.withColumn("bearing_dir", F.lit(-1))
        all_ints = all_ints_forward.unionByName(all_ints_reverse)

        intersecting_way_tag_data = spark.sql(
            f"""
            WITH inter_tags AS (
                SELECT
                    oui.node_id,
                    oui.way_id,
                    TRANSFORM(FILTER(oui.intersecting_ways, x-> x.way_id != oui.way_id), x -> x.highway_tag_value) AS inter_tags
                FROM stopsigns.{self.region_name}_intersections oui
            )
            SELECT
                node_id,
                way_id,
                ARRAY_CONTAINS(inter_tags, 'residential') AS inter_tag_residential,
                ARRAY_CONTAINS(inter_tags, 'service') AS inter_tag_service,
                ARRAY_CONTAINS(inter_tags, 'secondary') AS inter_tag_secondary,
                ARRAY_CONTAINS(inter_tags, 'primary') AS inter_tag_primary,
                ARRAY_CONTAINS(inter_tags, 'tertiary') AS inter_tag_tertiary,
                ARRAY_CONTAINS(inter_tags, 'motorway_link') AS inter_tag_motorway_link,
                ARRAY_CONTAINS(inter_tags, 'motorway') AS inter_tag_motorway,
                ARRAY_CONTAINS(inter_tags, 'unclassified') AS inter_tag_unclassified,
                inter_tags
            FROM inter_tags
        """
        )

        if ground_truth_cities:
            all_ints = self.add_ground_truth(all_ints, ground_truth_cities)
        for h3_res_3_index in h3_res_3_index_list:
            if h3_res_3_index in finished_h3:
                continue
            print(f"Combining {h3_res_3_index}...")
            ints_h3 = all_ints
            telemetry_inf = (
                self.get_telem_table(
                    f"stopsigns.{self.region_name}_inference_{self.hist_limit}_{h3_res_3_index}"
                )
                .select(
                    "way_id",
                    "node_id",
                    "bearing_dir",
                    "probability_of_stop_sign",
                    "<img>",
                    "trace_count",
                )
                .withColumnRenamed("probability_of_stop_sign", "telemetry_conf")
                .withColumnRenamed("<img>", "histogram_s3_link")
            )
            telemetry_with_ints = ints_h3.join(
                telemetry_inf, ["way_id", "node_id", "bearing_dir"], how="left"
            )
            trip_still_inf = self.get_trip_stills_table(
                f"stopsigns.{self.region_name}_trip_stills_detected_stop_signs_h3_res_3_{h3_res_3_index}"
            )
            trip_still_inf_grouped_int = trip_still_inf.groupBy(
                ["way_id", "node_id", "bearing_dir"]
            ).agg(
                F.collect_list(
                    F.struct(F.col("stop_sign_confidence"), F.col("stop_sign_bbox"))
                ).alias("trip_still_preds"),
                F.collect_list("s3url").alias("s3urls"),
            )
            telemetry_trip_still_combined = telemetry_with_ints.join(
                trip_still_inf_grouped_int,
                ["way_id", "node_id", "bearing_dir"],
                how="left",
            )
            agg_speed_data = spark.table(f"stopsigns.{self.region_name}_agg_speed_data")
            agg_speed_data = agg_speed_data.filter(
                agg_speed_data.hex_id_res_3 == h3_res_3_index
            )
            telemetry_trip_still_combined = telemetry_trip_still_combined.join(
                agg_speed_data,
                ["way_id", "node_id", "bearing_dir", "latitude", "longitude"],
                how="left",
            )

            tag_data = spark.sql(
                f"""
                SELECT
                    way_id,
                    IF(t.highway_tag == 'residential', 1, 0) AS tag_residential,
                    IF(t.highway_tag == 'service', 1, 0) AS tag_service,
                    IF(t.highway_tag == 'secondary', 1, 0) AS tag_secondary,
                    IF(t.highway_tag == 'primary', 1, 0) AS tag_primary,
                    IF(t.highway_tag == 'tertiary', 1, 0) AS tag_tertiary,
                    IF(t.highway_tag == 'motorway_link', 1, 0) AS tag_motorway_link,
                    IF(t.highway_tag == 'motorway', 1, 0) AS tag_motorway,
                    IF(t.highway_tag == 'unclassified', 1, 0) AS tag_unclassified,
                    t.highway_tag
                FROM stopsigns.{self.region_name}_way_tags t
                WHERE
                    t.hex_id_res_3 = '{h3_res_3_index}'
            """
            )

            telemetry_trip_still_combined = telemetry_trip_still_combined.join(
                tag_data, ["way_id"]
            )

            telemetry_trip_still_combined = telemetry_trip_still_combined.join(
                intersecting_way_tag_data, ["way_id", "node_id"]
            )
            telemetry_trip_still_combined = self.get_preds(
                telemetry_trip_still_combined
            )
            telemetry_trip_still_combined_with_reverse_dir = self.get_reverse_dir(
                telemetry_trip_still_combined
            )
            telemetry_trip_still_combined_with_reverse_dir.write.mode(
                "overwrite"
            ).option("overwriteSchema", "true").saveAsTable(
                f"stopsigns.combined_preds_{self.region_name}_hex_res_3_{h3_res_3_index}"
            )
            finished_h3 = finished_h3 + [h3_res_3_index]
            write_h3_table(h3_table_name, finished_h3)
            print("Done")
        drop_h3_table(h3_table_name)

    def get_reverse_dir(self, pred_sdf):
        null_preds = pred_sdf.filter(F.col("combined_prediction").isNull())
        null_preds.createOrReplaceTempView("null_preds")
        pred_sdf.createOrReplaceTempView("combined_dataset")
        null_pred_counterparts = spark.sql(
            """
            select
                np.*
            from combined_dataset csd
            join null_preds np on
                csd.way_id = np.way_id and
                csd.node_id = np.node_id and
                csd.bearing_dir = -1*np.bearing_dir
            where csd.combined_prediction is not null
        """
        )
        null_pred_counterparts.createOrReplaceTempView("null_pred_counterparts")
        dataset_without_counterparts = spark.sql(
            """
              select
                *
              from combined_dataset
              where struct(way_id, node_id, bearing_dir) not in (select way_id, node_id, bearing_dir from null_pred_counterparts)
          """
        )
        combined_dataset_sdf_all = dataset_without_counterparts.unionByName(
            null_pred_counterparts
        )
        return combined_dataset_sdf_all

    def union_to_one_table(self, mode="overwrite"):
        if mode == "ignore":
            return
        h3_res_3_index_list = (
            spark.table(f"stopsigns.{self.region_name}_telemetry_data")
            .select("hex_id_res_3")
            .dropDuplicates()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        print(h3_res_3_index_list)

        data_sdf = spark.table(
            f"stopsigns.combined_preds_{self.region_name}_hex_res_3_{h3_res_3_index_list[0]}"
        )
        for i in range(1, len(h3_res_3_index_list)):
            h3_res_3_index = h3_res_3_index_list[i]
            cur_h3_sdf = spark.table(
                f"stopsigns.combined_preds_{self.region_name}_hex_res_3_{h3_res_3_index}"
            )
            data_sdf = data_sdf.unionByName(cur_h3_sdf)
        data_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            f"stopsigns.combined_preds_{self.region_name}_all"
        )
