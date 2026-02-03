# Databricks notebook source

# COMMAND ----------

from dataclasses import dataclass, field

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, explode, from_json, when, lit, expr
from pyspark.sql.types import StringType


@dataclass
class OpenStreetMapDataProcessor:
    spark: SparkSession
    params: dict
    df_osm_maps_spark: any = field(init=False, default=None)
    df_nodes: any = field(init=False, default=None)
    df_nodes_lat_lng: any = field(init=False, default=None)
    df_osm_roads: any = field(init=False, default=None)
    external_bridge_data: any = field(init=False, default=None)
    unwanted_highways: list = field(
        init=False,
        default_factory=lambda: [
            "footway",
            "bridleway",
            "steps",
            "pedestrian",
            "cycleway",
            "path",
            "track",
        ],
    )

    def __post_init__(self):
        """
        The __post_init__ method is a special method in Python dataclasses. It is automatically called immediately
        after the dataclass is initialized. This method allows for additional setup or processing that needs to happen
        after the initial __init__ method is called.
        In this case, the __post_init__ method is used to perform a series of data processing steps,
        ensuring the object is fully prepared and all necessary operations are executed right the object creation.
        """
        self.read_data()
        self.filter_unwanted_highways()
        self.extract_node_ids()
        self.extract_oneway()
        self.extract_exit_node()
        self.extract_and_sort_lat_lon()
        self.extract_points()
        self.extract_locale()
        self.extract_max_height()
        self.fill_missing_max_height_from_external()
        self.extract_special_tags()
        self.create_nodes_dataframe()
        self.select_final_columns()

    def read_data(self):
        self.df_osm_roads = self.spark.read.table(self.params["osm_table_name"])
        # Map region names to external bridge data table names
        # If a region doesn't have external bridge data, external_bridge_data will remain None
        region_to_external_table = {
            "Europe": "safety_map_data.external_bridge_ways_eu",
            "United States of America": "safety_map_data.external_bridge_ways_usa",
            # Add other regions here as external bridge data becomes available
            # "Mexico": "safety_map_data.external_bridge_ways_mex",
            # "Canada": "safety_map_data.external_bridge_ways_can",
        }

        if self.params["region"] in region_to_external_table:
            try:
                self.external_bridge_data = self.spark.read.table(
                    region_to_external_table[self.params["region"]]
                )
            except Exception as e:
                # If table doesn't exist, log and continue without external bridge data
                print(
                    f"Warning: External bridge data table not found for region {self.params['region']}: {e}"
                )
                self.external_bridge_data = None

    def filter_unwanted_highways(self):
        self.df_osm_maps_spark = self.df_osm_roads.filter(
            ~col("highway").isin(self.unwanted_highways)
        )
        # Apply additional filtering based on filter_out_tags
        self.df_osm_maps_spark = self.df_osm_maps_spark.filter(
            ~tag_filter_udf(col("tags"))
        )

    def extract_node_ids(self):
        # Extract node IDs from the 'nodes' column using a custom UDF
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "nodes", extract_node_ids_udf(col("nodes"))
        )

    def extract_locale(self):
        # Extract the locale from the first node using a custom UDF
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "locale", extract_locale_from_coordinates_udf(col("coordinates"))
        )

    def extract_oneway(self):
        # Extract the oneway tag from the 'tags' column using a custom UDF
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "oneway", extract_oneway_udf(col("tags"))
        )

    def extract_exit_node(self):
        # Extract the exit node from the 'nodes' column using a custom UDF
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "exit_node", extract_exit_node_udf(col("oneway"), col("nodes"))
        )

    def extract_max_height(self):
        # Extract the maximum height restriction from the 'tags' column using a custom UDF
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "max_height", extract_maxheight_udf(col("tags"), col("locale"))
        )

    def fill_missing_max_height_from_external(self):
        if not self.external_bridge_data:
            return

        # Check if max_height_metres column exists in external bridge data
        has_max_height_metres = "max_height_metres" in self.external_bridge_data.columns
        # Check if max_height column exists in external bridge data
        has_max_height = "max_height" in self.external_bridge_data.columns

        joined_df = self.df_osm_maps_spark.alias("osm").join(
            self.external_bridge_data.alias("ext"),
            col("osm.way_id") == col("ext.matched_way_Id"),
            "left",
        )

        # Only create max_height_external and convert it if the max_height column exists
        if has_max_height:
            joined_df = joined_df.withColumn(
                "max_height_external", col("ext.max_height")
            ).withColumn(
                "max_height_external_converted",
                convert_external_max_height_udf(
                    col("max_height_external"), col("locale")
                ),
            )
        else:
            # If max_height doesn't exist, create null columns for consistency
            joined_df = joined_df.withColumn(
                "max_height_external", lit(None).cast(StringType())
            ).withColumn("max_height_external_converted", lit(None).cast(StringType()))

        # If max_height_metres exists, use it directly; otherwise use converted value
        if has_max_height_metres:
            joined_df = joined_df.withColumn(
                "external_height_in_meters",
                when(
                    col("ext.max_height_metres").isNotNull(),
                    col("ext.max_height_metres"),
                ).otherwise(col("max_height_external_converted")),
            )
        else:
            joined_df = joined_df.withColumn(
                "external_height_in_meters", col("max_height_external_converted")
            )

        # Final max_height: prefer OSM value, then external (either from max_height_metres or converted)
        joined_df = joined_df.withColumn(
            "final_max_height",
            when(col("osm.max_height").isNotNull(), col("osm.max_height")).otherwise(
                col("external_height_in_meters")
            ),
        )

        self.df_osm_maps_spark = (
            joined_df.withColumn(
                "final_max_height", col("final_max_height").cast(StringType())
            )
            .drop("max_height")
            .withColumnRenamed("final_max_height", "max_height")
            .drop(
                "max_height_external",
                "max_height_external_converted",
                "external_height_in_meters",
            )
            .select("osm.*", "max_height")
        )

    def extract_special_tags(self):
        # Extract special tags of interest - maxheight, tunnel, bridge from the 'tags' column using a custom UDF
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "tags", extract_special_tags_udf(col("tags"))
        )

    def extract_and_sort_lat_lon(self):
        # Extract and sort latitude and longitude coordinates using a custom UDF
        # Split the sorted_lat_lon into separate 'lat' and 'lon' columns
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "sorted_lat_lon", extract_and_sort_lat_lon_udf(col("nodes_lat_lng"))
        )
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "lat_degrees", col("sorted_lat_lon.lat")
        ).withColumn("lng_degrees", col("sorted_lat_lon.lon"))

    def extract_points(self):
        self.df_osm_maps_spark = self.df_osm_maps_spark.withColumn(
            "coordinates",
            extract_coordinates_to_points_udf(col("lat_degrees"), col("lng_degrees")),
        )

    def create_nodes_dataframe(self):
        all_roads = self.df_osm_roads.filter(
            ~col("highway").isin(self.unwanted_highways)
        )

        # Apply additional filtering based on filter_out_tags
        all_roads = all_roads.filter(~tag_filter_udf(col("tags")))

        self.df_nodes = (
            all_roads.select(all_roads.way_id, explode(all_roads.nodes).alias("node"))
            .select(
                col("way_id"), col("node.nodeId").alias("node_id"), col("node.index")
            )
            .dropDuplicates(["node_id"])
        )
        self.df_nodes_lat_lng = all_roads.select(
            all_roads.way_id, explode(all_roads.nodes_lat_lng).alias("node_lat_lng")
        )

    def select_final_columns(self):
        self.df_osm_maps_spark = self.df_osm_maps_spark.select(
            "way_id",
            "nodes",
            "max_height",
            "tags",
            "coordinates",
            "exit_node",
        )
