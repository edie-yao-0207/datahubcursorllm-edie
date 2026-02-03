# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    size,
    lit,
    collect_list,
    collect_set,
    flatten,
    row_number,
    coalesce,
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F


class DataFrameMatcher:
    """
    This class is responsible for matching road data from HERE Maps with road data from OSM.
    It performs several operations including interpolation of WKT lines, creation of H3 indices,
    exploding H3 indices, matching dataframes based on H3 indices, grouping matched data,
    ranking and filtering data, and finally combining dataframes to get a unified dataset.
    """

    def __init__(self, df_osm: DataFrame, df_here: DataFrame):
        self.df_osm_maps_spark = df_osm
        self.df_here_maps_spark = df_here
        self.matched_df_grouped = None
        self.df_here_bridges_tunnels = None
        self.df_all_roads = None
        self.process_dataframes()

    def process_dataframes(self):
        self.interpolate_data()
        self.create_h3_indices()
        self.explode_h3_indices()
        self.match_dataframes()
        self.group_matched_data()
        self.rank_and_filter_data()
        self.combine_dataframes()
        self.drop_duplicates()

    def interpolate_data(self):
        self.df_here_maps_spark = self.df_here_maps_spark.withColumn(
            "interpolated_wkt", interp_udf(col("wkt"))
        )
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "interpolated_wkt", interp_udf(col("wkt"))
        )

    def create_h3_indices(self):
        self.df_here_maps_spark = self.df_here_maps_spark.withColumn(
            "interpolated_h3",
            line_string_to_h3_str_udf(col("interpolated_wkt"), lit(13)),
        ).withColumn("interpolated_h3_size", size("interpolated_h3"))

        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "interpolated_h3",
            line_string_to_h3_str_udf(col("interpolated_wkt"), lit(13)),
        ).withColumn("interpolated_h3_size", size("interpolated_h3"))

    def explode_h3_indices(self):
        self.df_here_maps_spark = self.df_here_maps_spark.withColumn(
            "here_exploded_h3", explode(col("interpolated_h3"))
        )
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "osm_exploded_h3", explode(col("interpolated_h3"))
        )

    def match_dataframes(self):
        df_here_alias = self.df_here_maps_spark.alias("here")
        df_osm_alias = self.df_osm_maps_spark.alias("osm")

        matched_df = df_here_alias.join(
            df_osm_alias,
            col("osm.osm_exploded_h3") == col("here.here_exploded_h3"),
            how="inner",
        )

        self.matched_df_grouped = matched_df.groupBy(
            "here.link_id",
            "here.max_height",
            "osm.way_id",
            "here.interpolated_wkt",
            "osm.wkt",
            "osm.length",
        ).agg(
            collect_list("osm.osm_exploded_h3").alias("matched_h3"),
            collect_set("osm.tags").alias("tags"),
            collect_set("osm.nodes").alias("nodes"),
        )

    def group_matched_data(self):
        self.matched_df_grouped = (
            self.matched_df_grouped.withColumn("tags", flatten("tags"))
            .withColumn("nodes", flatten("nodes"))
            .withColumn("matched_h3_count", size(col("matched_h3")))
            .withColumn("unique_tags_count", count_unique_tags_udf(col("tags")))
            .withColumn(
                "matching_score", col("matched_h3_count") + col("unique_tags_count")
            )
        )

    def rank_and_filter_data(self):
        window_spec = Window.partitionBy("way_id").orderBy(col("length").desc())
        self.matched_df_grouped = (
            self.matched_df_grouped.withColumn(
                "row_num", row_number().over(window_spec)
            )
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

        window_spec = Window.partitionBy("link_id")
        self.matched_df_grouped = self.matched_df_grouped.withColumn(
            "max_matching_score", F.max("matching_score").over(window_spec)
        ).filter(col("matching_score") == col("max_matching_score"))

        self.df_here_bridges_tunnels = self.matched_df_grouped.select(
            "way_id", "max_height", "nodes", "length"
        )

    def combine_dataframes(self):
        df_here_bridges_tunnels = self.df_here_bridges_tunnels.alias(
            "here_bridges_tunnels"
        )
        df_osm_maps_spark = self.df_osm_maps_spark.alias("osm")

        combined_df = df_osm_maps_spark.join(
            df_here_bridges_tunnels,
            df_osm_maps_spark["way_id"] == df_here_bridges_tunnels["way_id"],
            how="left",
        )

        self.df_all_roads = (
            combined_df.withColumn(
                "nodes_", coalesce(col("here_bridges_tunnels.nodes"), col("osm.nodes"))
            )
            .withColumn(
                "length_",
                coalesce(col("here_bridges_tunnels.length"), col("osm.length")),
            )
            .withColumn(
                "max_height_",
                coalesce(col("here_bridges_tunnels.max_height"), col("osm.max_height")),
            )
            .select(
                col("osm.way_id").alias("way_id"),
                col("nodes_").alias("nodes"),
                col("length_").alias("length"),
                col("max_height_").alias("max_height"),
            )
        )

    def drop_duplicates(self):
        self.df_all_roads = self.df_all_roads.dropDuplicates(["way_id"])

    def show_combined_dataframe(self):
        self.df_all_roads.show()
